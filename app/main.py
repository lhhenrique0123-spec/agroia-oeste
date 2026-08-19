import os, csv, sqlite3, requests, threading, tempfile, re, time
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title='AgroIA Oeste')
DB='agro_assistente.db'
RESOURCE_ID='d30b30d7-e256-484e-9ab8-cd40974e1238'
CKAN_BASE='https://dados.agricultura.gov.br/api/3/action'
AGROFIT_URL=os.getenv('AGROFIT_CSV_URL','https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv')
OPEN_LLM_URL=os.getenv('OPEN_LLM_URL','').strip()
OPEN_LLM_MODEL=os.getenv('OPEN_LLM_MODEL','').strip()
_sync_lock=threading.Lock()

def db():
    c=sqlite3.connect(DB,timeout=30)
    c.row_factory=sqlite3.Row
    c.execute('CREATE TABLE IF NOT EXISTS produtos (registro TEXT, marca TEXT, ingrediente TEXT, classe TEXT, cultura TEXT, alvo TEXT)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cultura ON produtos(cultura)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_alvo ON produtos(alvo)')
    c.execute('CREATE TABLE IF NOT EXISTS sync_meta (id INTEGER PRIMARY KEY CHECK(id=1), ultima_sync TEXT, linhas INTEGER DEFAULT 0, status TEXT)')
    c.commit(); return c

def pick(row,*names):
    norm={str(k).strip().lower():v for k,v in row.items()}
    for n in names:
        if n.lower() in norm: return str(norm[n.lower()] or '').strip()
    return ''

def row_tuple(row):
    reg=pick(row,'Nº Registro','N° Registro','Registro','Numero Registro','Número Registro','nr_registro','numero_registro')
    marca=pick(row,'Marca Comercial','Produto Formulado','Nome Comercial','marca_comercial','produto_formulado')
    ing=pick(row,'Ingrediente Ativo','Ingrediente ativo','ingrediente_ativo')
    classe=pick(row,'Classe','Classe Agronômica','Classe Agronomica','classe_agronomica')
    cultura=pick(row,'Cultura','cultura')
    comum=pick(row,'Praga Nome Comum','Nome Comum','Alvo','praga_nome_comum','nome_comum')
    cient=pick(row,'Praga Nome Científico','Praga Nome Cientifico','praga_nome_cientifico')
    alvo=' — '.join(x for x in (comum,cient) if x)
    return (reg,marca,ing,classe,cultura,alvo) if (reg or marca) else None

def _set_meta(status,linhas=0):
    c=db(); c.execute('INSERT OR REPLACE INTO sync_meta(id,ultima_sync,linhas,status) VALUES(1,?,?,?)',(datetime.now(timezone.utc).isoformat(),linhas,status)); c.commit(); c.close()

def _replace_produtos(records):
    c=db()
    try:
        c.execute('BEGIN'); c.execute('DELETE FROM produtos')
        n=0; batch=[]
        for row in records:
            tup=row_tuple(row)
            if not tup: continue
            batch.append(tup); n+=1
            if len(batch)>=1000:
                c.executemany('INSERT INTO produtos VALUES (?,?,?,?,?,?)',batch); batch=[]
        if batch: c.executemany('INSERT INTO produtos VALUES (?,?,?,?,?,?)',batch)
        c.commit(); return n
    finally:
        c.close()

def sync_via_datastore():
    _set_meta('tentando API CKAN/DataStore',0)
    records=[]; offset=0; limit=5000; total=None
    while True:
        r=requests.get(f'{CKAN_BASE}/datastore_search',params={'resource_id':RESOURCE_ID,'limit':limit,'offset':offset},timeout=(10,35),headers={'User-Agent':'AgroIA-Oeste/2.1'})
        r.raise_for_status(); data=r.json()
        if not data.get('success'): raise RuntimeError('DataStore respondeu success=false')
        result=data.get('result') or {}; batch=result.get('records') or []
        if total is None: total=int(result.get('total') or 0)
        records.extend(batch); offset += len(batch)
        _set_meta(f'API CKAN: {offset}/{total or "?"}',offset)
        if not batch or (total is not None and offset>=total): break
        if offset>250000: raise RuntimeError('Limite de segurança da API excedido')
    if not records: raise RuntimeError('DataStore retornou zero registros')
    return _replace_produtos(records)

def sync_via_csv():
    path=None; _set_meta('baixando CSV oficial do MAPA',0)
    try:
        url=AGROFIT_URL
        try:
            meta=requests.get(f'{CKAN_BASE}/resource_show',params={'id':RESOURCE_ID},timeout=(8,20),headers={'User-Agent':'AgroIA-Oeste/2.1'})
            if meta.ok:
                j=meta.json(); url=((j.get('result') or {}).get('url')) or url
        except Exception: pass
        with requests.get(url,stream=True,timeout=(12,60),headers={'User-Agent':'AgroIA-Oeste/2.1','Accept':'text/csv,*/*'}) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False,suffix='.csv') as f:
                path=f.name; downloaded=0
                for chunk in r.iter_content(chunk_size=512*1024):
                    if chunk:
                        f.write(chunk); downloaded+=len(chunk)
                        if downloaded%(5*1024*1024)<512*1024: _set_meta(f'baixando CSV: {downloaded//(1024*1024)} MB',0)
        def rows_iter():
            with open(path,'r',encoding='utf-8-sig',errors='replace',newline='') as fh:
                first=fh.readline(); delim=';' if first.count(';')>first.count(',') else ','; fh.seek(0)
                for row in csv.DictReader(fh,delimiter=delim): yield row
        return _replace_produtos(rows_iter())
    finally:
        if path:
            try: os.unlink(path)
            except: pass

def sync_agrofit():
    if not _sync_lock.acquire(blocking=False): return
    try:
        _set_meta('iniciando sincronização',0); erros=[]
        try:
            n=sync_via_datastore(); _set_meta('ok • fonte: API CKAN/DataStore',n); return
        except Exception as e: erros.append('API: '+str(e)[:120])
        try:
            n=sync_via_csv(); _set_meta('ok • fonte: CSV oficial',n); return
        except Exception as e: erros.append('CSV: '+str(e)[:120])
        _set_meta('erro • '+' | '.join(erros),0)
    finally:
        _sync_lock.release()

def sync_background(): threading.Thread(target=sync_agrofit,daemon=True).start()
def periodic_sync():
    while True:
        time.sleep(24*60*60); sync_agrofit()

@app.on_event('startup')
def startup():
    db().close(); threading.Timer(8,sync_background).start(); threading.Thread(target=periodic_sync,daemon=True).start()

@app.get('/health')
def health():
    c=db(); count=c.execute('SELECT COUNT(*) n FROM produtos').fetchone()['n']; meta=c.execute('SELECT * FROM sync_meta WHERE id=1').fetchone(); c.close()
    return {'status':'ok','produtos_agrofit':count,'sync':dict(meta) if meta else None,'motor_ia':'AgroIA Local','modelo_aberto_configurado':bool(OPEN_LLM_URL)}

@app.post('/api/sync')
def api_sync(): sync_background(); return {'ok':True,'status':'sincronizacao_iniciada'}

@app.get('/api/sync/status')
def api_sync_status():
    c=db(); count=c.execute('SELECT COUNT(*) n FROM produtos').fetchone()['n']; meta=c.execute('SELECT * FROM sync_meta WHERE id=1').fetchone(); c.close(); return {'produtos':count,'meta':dict(meta) if meta else None}

@app.get('/api/produtos')
def produtos(q:str='',limit:int=50):
    c=db(); like=f'%{q}%'; rows=c.execute('SELECT * FROM produtos WHERE marca LIKE ? OR ingrediente LIKE ? OR cultura LIKE ? OR alvo LIKE ? LIMIT ?',(like,like,like,like,min(limit,200))).fetchall(); c.close(); return [dict(x) for x in rows]

def normaliza(s):
    return (s or '').lower().replace('ã','a').replace('á','a').replace('à','a').replace('â','a').replace('é','e').replace('ê','e').replace('í','i').replace('ó','o').replace('ô','o').replace('õ','o').replace('ú','u').replace('ç','c')

def detectar_cultura(q):
    mapa={'soja':['soja'],'milho':['milho'],'sorgo':['sorgo'],'algodão':['algodao','algodão'],'feijão':['feijao','feijão'],'trigo':['trigo'],'café':['cafe','café']}; nq=normaliza(q)
    for cultura,vars in mapa.items():
        if any(normaliza(v) in nq for v in vars): return cultura
    return ''

def mapa_contexto(pergunta,limite=12):
    q=normaliza(pergunta); cultura=detectar_cultura(pergunta)
    stop={'como','qual','quais','para','posso','usar','estou','com','uma','meu','minha','que','tem','de','da','do','na','no','em','resolver','tratar','controle','controlar'}
    termos=[t for t in re.findall(r'[a-z0-9-]+',q) if len(t)>=4 and t not in stop and normaliza(cultura)!=t]
    c=db(); rows=c.execute('SELECT * FROM produtos WHERE lower(cultura) LIKE ? LIMIT 8000',(f'%{cultura}%',)).fetchall() if cultura else c.execute('SELECT * FROM produtos LIMIT 8000').fetchall(); c.close(); scored=[]
    for r in rows:
        d=dict(r); alvo=normaliza(d.get('alvo','')); hay=normaliza(' '.join(str(v or '') for v in d.values())); score=0
        for t in termos:
            if t in alvo: score+=6
            elif t in hay: score+=2
        if cultura and normaliza(cultura) in normaliza(d.get('cultura','')): score+=2
        if score>0: scored.append((score,d))
    scored.sort(key=lambda x:x[0],reverse=True); out=[]; seen=set()
    for _,d in scored:
        key=(d.get('registro'),d.get('marca'),d.get('cultura'),d.get('alvo'))
        if key not in seen: seen.add(key); out.append(d)
        if len(out)>=limite: break
    return out,cultura,termos

def resposta_local(pergunta,prods,cultura,termos):
    if not prods:
        c=db(); count=c.execute('SELECT COUNT(*) n FROM produtos').fetchone()['n']; meta=c.execute('SELECT * FROM sync_meta WHERE id=1').fetchone(); c.close()
        if count==0:
            st=dict(meta)['status'] if meta else 'base ainda não sincronizada'
            return f'A base MAPA/AGROFIT ainda não está pronta ({st}). O sistema tenta a API CKAN e, se necessário, o CSV oficial. Aguarde o status mudar ou use “Sincronizar agora”.'
        alvo=', '.join(termos[:4]) if termos else 'o problema informado'
        return f'Não encontrei correspondência segura no AGROFIT para {alvo}' + (f' em {cultura}' if cultura else '') + '. Informe cultura e nome da doença/praga/planta daninha para refinar. Não vou inventar produto quando a base oficial não retornar resultado.'
    linhas=[]; usados=set()
    for p in prods:
        key=p.get('marca') or p.get('ingrediente')
        if key in usados: continue
        usados.add(key); linhas.append(f"• {p.get('marca') or 'Nome comercial não informado'} — IA: {p.get('ingrediente') or 'n/i'} — Registro MAPA: {p.get('registro') or 'n/i'} — Alvo: {p.get('alvo') or 'n/i'}")
        if len(linhas)>=6: break
    alvo=', '.join(termos[:3]) if termos else 'o alvo informado'; cab=f'Encontrei {len(prods)} registros relacionados no MAPA/AGROFIT para {alvo}' + (f' em {cultura}' if cultura else '') + '.'
    manejo='\n\nLeitura agronômica: confirme primeiro o diagnóstico e a severidade no talhão. Priorize manejo integrado, rotação de mecanismos de ação e momento correto de aplicação.'
    aviso='\n\nImportante: dose, número de aplicações, intervalo, mistura e tecnologia de aplicação devem ser confirmados no rótulo/bula vigente e em receituário agronômico.'
    return cab+'\n\nOpções encontradas:\n'+'\n'.join(linhas)+manejo+aviso

def chamar_modelo_aberto(pergunta,contexto):
    if not OPEN_LLM_URL: return None
    try:
        payload={'model':OPEN_LLM_MODEL or 'local','messages':[{'role':'system','content':'Você é a AgroIA Oeste. Responda em português, usando somente o contexto MAPA/AGROFIT fornecido para citar produtos. Nunca invente registro, dose ou indicação.'},{'role':'user','content':f'Pergunta: {pergunta}\n\nContexto:\n{contexto}'}],'temperature':0.2}
        r=requests.post(OPEN_LLM_URL,json=payload,timeout=90); r.raise_for_status(); data=r.json()
        if 'choices' in data: return data['choices'][0]['message']['content']
        if 'message' in data and isinstance(data['message'],dict): return data['message'].get('content')
    except Exception: return None

class ChatInput(BaseModel): mensagem:str

@app.post('/api/chat')
def chat(payload:ChatInput):
    pergunta=payload.mensagem.strip()
    if not pergunta: return {'ok':False,'erro':'Digite uma pergunta.'}
    prods,cultura,termos=mapa_contexto(pergunta); contexto='\n'.join(f"Registro {p['registro'] or 'n/i'} | Marca {p['marca'] or 'n/i'} | IA {p['ingrediente'] or 'n/i'} | Classe {p['classe'] or 'n/i'} | Cultura {p['cultura'] or 'n/i'} | Alvo {p['alvo'] or 'n/i'}" for p in prods)
    resposta=chamar_modelo_aberto(pergunta,contexto) or resposta_local(pergunta,prods,cultura,termos)
    return {'ok':True,'motor':'AgroIA Local','resposta':resposta,'produtos_mapa_consultados':len(prods),'produtos':prods[:6]}

HTML='''<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AgroIA Oeste</title><style>*{box-sizing:border-box}body{margin:0;font-family:Arial;background:#f4f7f4;color:#173522}.top{background:linear-gradient(135deg,#003d26,#087b3f);color:#fff;padding:28px 5%}.wrap{max-width:1100px;margin:auto;padding:22px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}.card{background:#fff;border-radius:16px;padding:20px;box-shadow:0 3px 15px #0001}.btn{background:#07823f;color:#fff;border:0;border-radius:10px;padding:11px 16px;font-weight:bold;cursor:pointer}.badge{display:inline-block;background:#dff3e5;color:#08612f;padding:5px 9px;border-radius:20px;font-size:12px;font-weight:bold}.messages{height:360px;overflow:auto;background:#f7faf8;border:1px solid #dfe9e2;border-radius:13px;padding:14px}.msg{padding:11px 13px;border-radius:12px;margin:8px 0;max-width:90%;white-space:pre-wrap;line-height:1.4}.user{background:#087b3f;color:white;margin-left:auto}.bot{background:white;border:1px solid #dce7df}.row{display:flex;gap:8px;margin-top:10px}.row input{flex:1;padding:13px;border:1px solid #cad8ce;border-radius:10px}</style></head><body><div class="top"><h1>🌱 AgroIA Oeste</h1><p>Assistente agronômico conectado ao MAPA/AGROFIT</p></div><div class="wrap"><div class="grid"><div class="card"><span class="badge">SEM API PAGA</span><h3>🤖 AgroIA Local</h3><p>Motor próprio de consulta e resposta com base no MAPA/AGROFIT.</p></div><div class="card"><h3>🧪 Produtos MAPA</h3><p id="status">Verificando sincronização...</p><button class="btn" onclick="syncMapa()">Sincronizar agora</button></div><div class="card"><h3>🚜 Fazendas</h3><p>Histórico e gestão da lavoura.</p></div><div class="card"><h3>🛒 Onde Comprar</h3><p>Estrutura para revendas, estoque e preços.</p></div></div><div class="card" style="margin-top:18px"><h2>🤖 AgroIA Local</h2><div id="msgs" class="messages"><div class="msg bot">Olá! Eu respondo sem depender da OpenAI e cruzo sua pergunta com a base MAPA/AGROFIT. A sincronização agora usa API CKAN e CSV como fallback.</div></div><div class="row"><input id="q" placeholder="Ex.: Estou com mancha-alvo na soja no V8. O que existe registrado?" onkeydown="if(event.key==='Enter')enviar()"><button class="btn" onclick="enviar()">Enviar</button></div><small>AgroIA Local v2 • confirme rótulo/bula e receituário agronômico.</small></div></div><script>async function status(){try{let r=await fetch('/api/sync/status');let j=await r.json();document.getElementById('status').textContent=j.produtos+' registros/linhas carregados'+(j.meta?' • '+j.meta.status:'')}catch(e){document.getElementById('status').textContent='Falha ao consultar status'}}async function syncMapa(){document.getElementById('status').textContent='Sincronização iniciada...';await fetch('/api/sync',{method:'POST'});setTimeout(status,2500)}function add(t,c){let d=document.createElement('div');d.className='msg '+c;d.textContent=t;document.getElementById('msgs').appendChild(d);d.scrollIntoView()}async function enviar(){let i=document.getElementById('q'),q=i.value.trim();if(!q)return;add(q,'user');i.value='';add('Consultando MAPA/AGROFIT...','bot');let box=document.getElementById('msgs'),last=box.lastChild;try{let r=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mensagem:q})});let j=await r.json();last.textContent=j.ok?j.resposta:('Erro: '+(j.erro||'não foi possível responder'))}catch(e){last.textContent='Não foi possível consultar agora.'}}status();setInterval(status,5000)</script></body></html>'''

@app.get('/',response_class=HTMLResponse)
def home(): return HTML
