import os, csv, io, sqlite3, requests, threading, re
from datetime import datetime, timezone
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="AgroIA Oeste")
DB = "agro_assistente.db"
AGROFIT_URL = os.getenv("AGROFIT_CSV_URL", "https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.6")


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE IF NOT EXISTS produtos (registro TEXT, marca TEXT, ingrediente TEXT, classe TEXT, cultura TEXT, alvo TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prod_marca ON produtos(marca)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prod_cultura ON produtos(cultura)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prod_alvo ON produtos(alvo)")
    c.execute("CREATE TABLE IF NOT EXISTS fazendas (id INTEGER PRIMARY KEY, nome TEXT, cidade TEXT, area REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS sync_meta (id INTEGER PRIMARY KEY CHECK(id=1), ultima_sync TEXT, linhas INTEGER DEFAULT 0, status TEXT)")
    c.commit()
    return c


def pick(row, *names):
    norm = {str(k).strip().lower(): v for k, v in row.items()}
    for n in names:
        if n.lower() in norm:
            return str(norm[n.lower()] or "").strip()
    return ""


def sync_agrofit():
    c = db()
    try:
        c.execute("INSERT OR REPLACE INTO sync_meta(id,ultima_sync,linhas,status) VALUES(1,?,?,?)", (datetime.now(timezone.utc).isoformat(), 0, "sincronizando"))
        c.commit()
        r = requests.get(AGROFIT_URL, timeout=120, headers={"User-Agent": "AgroIA-Oeste/1.0"})
        r.raise_for_status()
        text = r.content.decode("utf-8-sig", errors="replace")
        sample = text[:12000]
        delim = ";" if sample.count(";") > sample.count(",") else ","
        rows = csv.DictReader(io.StringIO(text), delimiter=delim)
        c.execute("BEGIN")
        c.execute("DELETE FROM produtos")
        n = 0
        batch = []
        for row in rows:
            reg = pick(row, "Nº Registro", "N° Registro", "Registro", "Numero Registro", "Número Registro")
            marca = pick(row, "Marca Comercial", "Produto Formulado", "Nome Comercial")
            ing = pick(row, "Ingrediente Ativo", "Ingrediente ativo")
            classe = pick(row, "Classe", "Classe Agronômica", "Classe Agronomica")
            cultura = pick(row, "Cultura")
            alvo_comum = pick(row, "Praga Nome Comum", "Nome Comum", "Alvo")
            alvo_cient = pick(row, "Praga Nome Científico", "Praga Nome Cientifico")
            alvo = " — ".join(x for x in [alvo_comum, alvo_cient] if x)
            if reg or marca:
                batch.append((reg, marca, ing, classe, cultura, alvo))
                n += 1
            if len(batch) >= 1000:
                c.executemany("INSERT INTO produtos VALUES (?,?,?,?,?,?)", batch)
                batch.clear()
        if batch:
            c.executemany("INSERT INTO produtos VALUES (?,?,?,?,?,?)", batch)
        now = datetime.now(timezone.utc).isoformat()
        c.execute("INSERT OR REPLACE INTO sync_meta(id,ultima_sync,linhas,status) VALUES(1,?,?,?)", (now, n, "ok"))
        c.commit()
        return {"ok": True, "linhas": n, "ultima_sync": now}
    except Exception as e:
        c.rollback()
        c.execute("INSERT OR REPLACE INTO sync_meta(id,ultima_sync,linhas,status) VALUES(1,?,?,?)", (datetime.now(timezone.utc).isoformat(), 0, f"erro: {str(e)[:180]}"))
        c.commit()
        return {"ok": False, "erro": str(e)}
    finally:
        c.close()


def ensure_sync_background():
    c = db()
    count = c.execute("SELECT COUNT(*) n FROM produtos").fetchone()["n"]
    c.close()
    if count == 0:
        threading.Thread(target=sync_agrofit, daemon=True).start()


@app.on_event("startup")
def startup():
    db().close()
    ensure_sync_background()


@app.get("/health")
def health():
    c = db()
    count = c.execute("SELECT COUNT(*) n FROM produtos").fetchone()["n"]
    meta = c.execute("SELECT * FROM sync_meta WHERE id=1").fetchone()
    c.close()
    return {"status": "ok", "app": "AgroIA Oeste", "produtos_agrofit": count, "sync": dict(meta) if meta else None, "ia_configurada": bool(OPENAI_API_KEY)}


@app.post("/api/sync")
def sync():
    return sync_agrofit()


@app.get("/api/sync/status")
def sync_status():
    c = db()
    count = c.execute("SELECT COUNT(*) n FROM produtos").fetchone()["n"]
    meta = c.execute("SELECT * FROM sync_meta WHERE id=1").fetchone()
    c.close()
    return {"produtos": count, "meta": dict(meta) if meta else None}


@app.get("/api/produtos")
def produtos(q: str = "", limit: int = 50):
    c = db()
    like = f"%{q}%"
    rows = c.execute("SELECT * FROM produtos WHERE marca LIKE ? OR ingrediente LIKE ? OR cultura LIKE ? OR alvo LIKE ? LIMIT ?", (like, like, like, like, min(limit, 200))).fetchall()
    c.close()
    return [dict(x) for x in rows]


def mapa_contexto(pergunta: str, limite: int = 18):
    q = pergunta.lower()
    culturas = ["soja", "milho", "sorgo", "algodão", "algodao", "feijão", "feijao", "trigo", "café", "cafe"]
    cultura = next((x for x in culturas if x in q), "")
    stop = {"como", "qual", "quais", "para", "posso", "usar", "estou", "com", "uma", "meu", "minha", "que", "tem", "de", "da", "do", "na", "no", "em", "e", "o", "a"}
    termos = [t for t in re.findall(r"[a-záàâãéêíóôõúç0-9-]+", q) if len(t) >= 4 and t not in stop]
    c = db()
    if cultura:
        rows = c.execute("SELECT * FROM produtos WHERE lower(cultura) LIKE ? LIMIT 6000", (f"%{cultura}%",)).fetchall()
    else:
        rows = c.execute("SELECT * FROM produtos LIMIT 6000").fetchall()
    c.close()
    scored = []
    for r in rows:
        d = dict(r)
        hay = " ".join(str(v or "") for v in d.values()).lower()
        score = sum(3 if t in str(d.get("alvo", "")).lower() else 1 for t in termos if t in hay)
        if cultura and cultura in str(d.get("cultura", "")).lower():
            score += 2
        if score > 0:
            scored.append((score, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    seen = set()
    out = []
    for _, d in scored:
        key = (d.get("registro"), d.get("marca"), d.get("cultura"), d.get("alvo"))
        if key not in seen:
            seen.add(key)
            out.append(d)
        if len(out) >= limite:
            break
    return out


class ChatInput(BaseModel):
    mensagem: str


def extract_output_text(data):
    texts = []
    for item in data.get("output", []):
        if item.get("type") == "message":
            for part in item.get("content", []):
                if part.get("type") == "output_text" and part.get("text"):
                    texts.append(part["text"])
    return "\n".join(texts).strip()


@app.post("/api/chat")
def chat(payload: ChatInput):
    pergunta = payload.mensagem.strip()
    if not pergunta:
        return {"ok": False, "erro": "Digite uma pergunta."}
    produtos = mapa_contexto(pergunta)
    contexto = "\n".join(
        f"- Registro MAPA: {p['registro'] or 'n/i'} | Marca: {p['marca'] or 'n/i'} | Ingrediente ativo: {p['ingrediente'] or 'n/i'} | Classe: {p['classe'] or 'n/i'} | Cultura: {p['cultura'] or 'n/i'} | Alvo: {p['alvo'] or 'n/i'}"
        for p in produtos
    ) or "Nenhum produto relacionado foi encontrado na base local sincronizada do AGROFIT para os termos da pergunta."

    if not OPENAI_API_KEY:
        return {
            "ok": False,
            "configuracao_pendente": True,
            "resposta": "A consulta ao MAPA já está integrada, mas a chave da IA ainda precisa ser configurada no servidor. A base encontrou %d registros relacionados." % len(produtos),
            "produtos": produtos[:8]
        }

    instructions = """Você é o Assistente Agronômico do AgroIA Oeste, focado no Oeste da Bahia. Responda em português do Brasil, de forma prática, técnica e curta. Use os dados AGROFIT fornecidos como fonte regulatória para produtos registrados. Nunca invente nome comercial, registro MAPA, cultura, alvo ou ingrediente ativo. Não invente dose, intervalo de segurança, número de aplicações ou mistura em tanque quando essas informações não estiverem no contexto. Quando a pergunta envolver recomendação de defensivo, deixe claro que a decisão final deve seguir rótulo/bula vigente, receituário agronômico e profissional habilitado. Você pode explicar manejo integrado, fisiologia e raciocínio agronômico geral, mas diferencie claramente conhecimento geral dos dados oficiais do MAPA."""
    user_input = f"Pergunta do usuário: {pergunta}\n\nDADOS SINCRONIZADOS DO MAPA/AGROFIT RELACIONADOS À PERGUNTA:\n{contexto}"
    try:
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": OPENAI_MODEL, "instructions": instructions, "input": user_input},
            timeout=90,
        )
        if r.status_code >= 400:
            return {"ok": False, "erro": f"OpenAI API: {r.status_code}", "detalhe": r.text[:300], "produtos": produtos[:8]}
        data = r.json()
        resposta = extract_output_text(data)
        return {"ok": True, "resposta": resposta or "Não consegui gerar uma resposta agora.", "produtos_mapa_consultados": len(produtos), "produtos": produtos[:8]}
    except Exception as e:
        return {"ok": False, "erro": str(e), "produtos": produtos[:8]}


@app.get("/api/assistente")
def assistente(cultura: str = Query("soja"), alvo: str = Query("")):
    c = db()
    rows = c.execute("SELECT DISTINCT registro,marca,ingrediente,classe,cultura,alvo FROM produtos WHERE cultura LIKE ? AND alvo LIKE ? LIMIT 12", (f"%{cultura}%", f"%{alvo}%")).fetchall()
    c.close()
    return {"consulta": {"cultura": cultura, "alvo": alvo}, "produtos": [dict(x) for x in rows], "aviso": "Consulte rótulo/bula vigente e profissional habilitado antes de qualquer aplicação."}


HTML = '''<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AgroIA Oeste</title><style>*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;background:#f4f7f4;color:#173522}.top{background:linear-gradient(135deg,#003d26,#087b3f);color:white;padding:28px 5%}.top h1{margin:0;font-size:30px}.wrap{max-width:1100px;margin:auto;padding:22px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}.card{background:white;border-radius:16px;padding:20px;box-shadow:0 3px 15px #0001;border:1px solid #e5eee7}.card h3{margin:6px 0;color:#075d34}.ico{font-size:30px}.btn{background:#07823f;color:white;border:0;border-radius:10px;padding:11px 16px;font-weight:bold;cursor:pointer}.btn:disabled{opacity:.6}.chatbox{margin-top:18px}.messages{height:340px;overflow:auto;background:#f7faf8;border:1px solid #dfe9e2;border-radius:13px;padding:14px}.msg{padding:11px 13px;border-radius:12px;margin:8px 0;max-width:88%;white-space:pre-wrap;line-height:1.4}.user{background:#087b3f;color:white;margin-left:auto}.bot{background:white;border:1px solid #dce7df}.chatrow{display:flex;gap:8px;margin-top:10px}.chatrow input{flex:1;padding:13px;border-radius:10px;border:1px solid #cad8ce;font-size:15px}.statusok{color:#087b3f;font-weight:bold}.statuswarn{color:#9a6500}.market{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.quote{background:#edf8f0;border-radius:12px;padding:14px}.tag{font-size:12px;background:#dff3e5;padding:4px 8px;border-radius:20px;color:#08612f}.foot{color:#68766d;font-size:12px;margin-top:20px}@media(max-width:600px){.market{grid-template-columns:1fr}.top h1{font-size:25px}.messages{height:300px}}</style></head><body><div class="top"><h1>🌱 AgroIA Oeste</h1><p>Assistente agronômico conectado ao MAPA/AGROFIT</p></div><div class="wrap"><div class="grid"><div class="card"><div class="ico">🤖</div><h3>Assistente IA</h3><p>Conversa com IA usando os registros sincronizados do MAPA como base regulatória.</p></div><div class="card"><div class="ico">🧪</div><h3>Produtos MAPA</h3><p id="mapaResumo">Verificando base...</p><button class="btn" onclick="syncMapa()">Sincronizar MAPA</button></div><div class="card"><div class="ico">🚜</div><h3>Fazendas</h3><p>Propriedades, talhões e histórico da lavoura.</p></div><div class="card"><div class="ico">🛒</div><h3>Onde Comprar</h3><p>Estrutura para revendas, estoque e preços.</p></div></div><div class="card chatbox"><h2>🤖 Assistente IA</h2><div class="messages" id="messages"><div class="msg bot">Olá! Pergunte sobre uma cultura, doença, praga ou manejo. Quando a pergunta envolver defensivos, eu consulto primeiro a base sincronizada do MAPA/AGROFIT.</div></div><div class="chatrow"><input id="pergunta" placeholder="Ex.: Estou com mancha-alvo na soja. Quais opções registradas existem?" onkeydown="if(event.key==='Enter')enviar()"><button class="btn" id="send" onclick="enviar()">Enviar</button></div><p style="font-size:12px;color:#68766d">A IA não substitui receituário agronômico. Confirme sempre rótulo/bula vigente.</p></div><h2>📈 Mercado</h2><div class="market"><div class="quote"><span class="tag">SOJA</span><h3>Soja</h3><p>Tendências e cotações</p></div><div class="quote"><span class="tag">MILHO</span><h3>Milho</h3><p>Tendências e cotações</p></div><div class="quote"><span class="tag">SORGO</span><h3>Sorgo</h3><p>Tendências e cotações</p></div></div><div class="card" style="margin-top:16px"><h3>🔄 Sincronização MAPA</h3><p id="status">Verificando...</p><button class="btn" onclick="syncMapa()">Sincronizar agora</button></div><p class="foot">Fonte regulatória de produtos: MAPA/AGROFIT. Informações de aplicação devem ser confirmadas em rótulo/bula vigente e com profissional habilitado.</p></div><script>
function addMsg(text,cls){let m=document.getElementById('messages');let d=document.createElement('div');d.className='msg '+cls;d.textContent=text;m.appendChild(d);m.scrollTop=m.scrollHeight}
async function status(){try{let r=await fetch('/api/sync/status');let j=await r.json();let meta=j.meta||{};let txt=j.produtos+' registros/linhas carregados';document.getElementById('mapaResumo').textContent=txt;document.getElementById('status').innerHTML=meta.status==='ok'?'<span class="statusok">✓ MAPA sincronizado</span> — '+txt:'<span class="statuswarn">'+(meta.status||'Aguardando sincronização')+'</span> — '+txt}catch(e){document.getElementById('status').textContent='Não foi possível consultar o status.'}}
async function syncMapa(){let s=document.getElementById('status');s.textContent='Sincronizando com o MAPA/AGROFIT...';try{let r=await fetch('/api/sync',{method:'POST'});let j=await r.json();s.textContent=j.ok?'✓ Sincronização concluída: '+j.linhas+' linhas processadas.':'Falha: '+(j.erro||'erro desconhecido');status()}catch(e){s.textContent='Não foi possível sincronizar agora.'}}
async function enviar(){let inp=document.getElementById('pergunta'),b=document.getElementById('send'),q=inp.value.trim();if(!q)return;addMsg(q,'user');inp.value='';b.disabled=true;addMsg('Consultando MAPA e preparando resposta...','bot');let m=document.getElementById('messages'),loading=m.lastChild;try{let r=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mensagem:q})});let j=await r.json();loading.remove();addMsg(j.resposta||('Erro: '+(j.erro||'não foi possível responder')),'bot')}catch(e){loading.remove();addMsg('Não foi possível falar com o assistente agora.','bot')}b.disabled=false;inp.focus()}
status();setInterval(status,15000);
</script></body></html>'''


@app.get("/", response_class=HTMLResponse)
def home():
    return HTML
