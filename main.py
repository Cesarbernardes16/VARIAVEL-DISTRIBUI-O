import os
from fastapi import FastAPI, Request
from fastapi.responses import Response
from dotenv import load_dotenv
from supabase import create_client, Client

# Importa os nossos novos routers
from routers import xadrez, incentivo, metas

load_dotenv()

app = FastAPI()

# Configuração global do Supabase (pode ser partilhada)
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# "Monta" as nossas abas na aplicação principal
# Adiciona o estado da app a cada request para que os routers possam usar
@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.supabase = supabase
    response = await call_next(request)
    return response

app.include_router(xadrez.router)
app.include_router(incentivo.router)
app.include_router(metas.router)

# Rota do Favicon (continua aqui)
@app.get("/favicon.ico", include_in_schema=False)
async def favicon_route():
    return Response(status_code=204)