import os, csv, io, sqlite3, requests
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

app = FastAPI(title="AgroIA Oeste")
DB="agro_assistente.db"
AGROFIT_URL=os.getenv("AGROFIT_CSV_URL","https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f/resource/d30b30d7-e256-484e-9ab8-cd40974e1238/download/agrofitprodutosformulados.csv")

def db():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
    c.execute("CREATE TABLE IF NOT EXISTS produtos (registro TEXT, marca TEXT, ingrediente TEXT, classe TEXT, cultura TEXT, alvo TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS fazendas (id INTEGER PRIMARY KEY, nome TEXT, cidade TEXT, area REAL)")
    c.commit(); return c

def pick(row,*names):
    norm={str(k).strip().lower():v for k,v in row.items()}
    for n in names:
        if n.lower() in norm: return str(norm[n.lower()] or "").strip()
    return ""

@app.get("/health")
def health(): return {"status":"ok","app":"AgroIA Oeste"}

@app.post("/api/sync")
def sync():
    r=requests.get(AGROFIT_URL,timeout=60); r.raise_for_status()
    text=r.content.decode("utf-8-sig",errors="replace")
    sample=text[:4000]; delim=";" if sample.count(";")>sample.count(",") else ","
    rows=csv.DictReader(io.StringIO(text),delimiter=delim)
    c=db(); c.execute("DELETE FROM produtos"); n=0
    for row in rows:
        reg=pick(row,"Nº Registro","Registro","Numero Registro","Número Registro")
        marca=pick(row,"Marca Comercial","Produto Formulado","Nome Comercial")
        ing=pick(row,"Ingrediente Ativo","Ingrediente ativo")
        classe=pick(row,"Classe","Classe Agronômica")
        cultura=pick(row,"Cultura")
        alvo=pick(row,"Praga Nome Comum","Nome Comum","Praga Nome Científico","Alvo")
        if reg or marca:
            c.execute("INSERT INTO produtos VALUES (?,?,?,?,?,?)",(reg,marca,ing,classe,cultura,alvo)); n+=1
    c.commit(); c.close(); return {"ok":True,"linhas":n}

@app.get("/api/produtos")
def produtos(q:str="",limit:int=50):
    c=db(); like=f"%{q}%"; rows=c.execute("SELECT * FROM produtos WHERE marca LIKE ? OR ingrediente LIKE ? OR cultura LIKE ? OR alvo LIKE ? LIMIT ?",(like,like,like,like,limit)).fetchall(); c.close(); return [dict(x) for x in rows]

@app.get("/api/assistente")
def assistente(cultura:str=Query("soja"), alvo:str=Query("")):
    c=db(); rows=c.execute("SELECT DISTINCT registro,marca,ingrediente,classe,cultura,alvo FROM produtos WHERE cultura LIKE ? AND alvo LIKE ? LIMIT 12",(f"%{cultura}%",f"%{alvo}%")).fetchall(); c.close()
    return {"consulta":{"cultura":cultura,"alvo":alvo},"produtos":[dict(x) for x in rows],"aviso":"Consulte rótulo/bula vigente e profissional habilitado antes de qualquer aplicação."}

HTML='''<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AgroIA Oeste</title><style>*{box-sizing:border-box}body{margin:0;font-family:Arial;background:#f4f7f4;color:#173522}.top{background:linear-gradient(135deg,#003d26,#087b3f);color:white;padding:28px 5%}.top h1{margin:0;font-size:30px}.wrap{max-width:1100px;margin:auto;padding:22px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}.card{background:white;border-radius:16px;padding:20px;box-shadow:0 3px 15px #0001;border:1px solid #e5eee7}.card h3{margin:6px 0;color:#075d34}.ico{font-size:30px}.btn{background:#07823f;color:white;border:0;border-radius:10px;padding:11px 16px;font-weight:bold;cursor:pointer}.market{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.quote{background:#edf8f0;border-radius:12px;padding:14px}.quote b{font-size:19px}.tag{font-size:12px;background:#dff3e5;padding:4px 8px;border-radius:20px;color:#08612f}.foot{color:#68766d;font-size:12px;margin-top:20px}@media(max-width:600px){.market{grid-template-columns:1fr}.top h1{font-size:25px}}</style></head><body><div class="top"><h1>🌱 AgroIA Oeste</h1><p>Seu assistente agronômico inteligente</p></div><div class="wrap"><h2>Bom dia! 👋</h2><div class="grid"><div class="card"><div class="ico">🤖</div><h3>Assistente IA</h3><p>Consulte cultura e alvo usando dados sincronizados do MAPA/AGROFIT.</p></div><div class="card"><div class="ico">🧪</div><h3>Produtos MAPA</h3><p>Catálogo de defensivos e indicações registradas.</p><button class="btn" onclick="sync()">Sincronizar MAPA</button></div><div class="card"><div class="ico">🚜</div><h3>Fazendas</h3><p>Propriedades, talhões e histórico da lavoura.</p></div><div class="card"><div class="ico">🛒</div><h3>Onde Comprar</h3><p>Estrutura preparada para revendas, estoque e preços.</p></div></div><h2>📈 Mercado</h2><div class="market"><div class="quote"><span class="tag">SOJA</span><h3>Soja</h3><b>Tendências e cotações</b><p>Oeste da Bahia • Chicago • dólar • fundamentos</p></div><div class="quote"><span class="tag">MILHO</span><h3>Milho</h3><b>Tendências e cotações</b><p>Preço regional • oferta • demanda • exportação</p></div><div class="quote"><span class="tag">SORGO</span><h3>Sorgo</h3><b>Tendências e cotações</b><p>Preço regional • relação com milho • demanda</p></div></div><div class="card" style="margin-top:16px"><h3>🔄 Sincronização MAPA</h3><p id="status">Clique para buscar a base pública do AGROFIT.</p><button class="btn" onclick="sync()">Sincronizar agora</button></div><p class="foot">Protótipo AgroIA Oeste. Informações agronômicas devem ser confirmadas em rótulo/bula vigente e com profissional habilitado.</p></div><script>async function sync(){let s=document.getElementById('status');s.textContent='Sincronizando...';try{let r=await fetch('/api/sync',{method:'POST'});let j=await r.json();s.textContent=j.ok?'Sincronização concluída: '+j.linhas+' linhas processadas.':'Falha na sincronização.'}catch(e){s.textContent='Não foi possível sincronizar agora.'}}</script></body></html>'''

@app.get("/",response_class=HTMLResponse)
def home(): return HTML
