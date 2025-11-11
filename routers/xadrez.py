import datetime
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from typing import Optional
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa a nossa lógica partilhada
from core.database import get_dados_apurados
from core.analysis import gerar_dashboard_e_mapas

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Função para obter o cliente Supabase do estado da request
def get_supabase(request: Request) -> Client:
    return request.state.supabase

# Função de processamento síncrono (para o thread pool)
def processar_xadrez_sincrono(df, view_mode):
    resumo_viagens, dashboard_equipas = [], None
    
    if view_mode == 'equipas_fixas':
        resultado_xadrez = gerar_dashboard_e_mapas(df)
        dashboard_equipas = resultado_xadrez["dashboard_data"]
    else: 
        colunas_resumo = ['MAPA', 'MOTORISTA', 'COD', 'MOTORISTA_2', 'COD_2', 'AJUDANTE_1', 'CODJ_1', 'AJUDANTE_2', 'CODJ_2', 'AJUDANTE_3', 'CODJ_3']
        colunas_existentes = [col for col in colunas_resumo if col in df.columns]
        resumo_df = df[colunas_existentes].sort_values(by='MOTORISTA' if 'MOTORISTA' in colunas_existentes else colunas_existentes[0])
        resumo_df.fillna('', inplace=True)
        resumo_viagens = resumo_df.to_dict('records')
        
    return resumo_viagens, dashboard_equipas

@router.get("/")
async def ler_relatorio_xadrez(
    request: Request, 
    view_mode: str = "equipas_fixas",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    search_query: Optional[str] = None,
    supabase: Client = Depends(get_supabase)
):
    
    hoje = datetime.date.today()
    data_inicio = data_inicio or hoje.replace(day=1).isoformat()
    data_fim = data_fim or hoje.isoformat()
    search_str = search_query or ""
    
    resumo_viagens, dashboard_equipas = [], None

    # --- ALTERAÇÃO: Chamar get_dados_apurados (síncrono) no threadpool ---
    df, error_message = await run_in_threadpool(
        get_dados_apurados, 
        supabase, 
        data_inicio, 
        data_fim, 
        search_str
    )
    
    # 2. Processar dados (em thread pool)
    if error_message is None and df is not None:
        resumo_viagens, dashboard_equipas = await run_in_threadpool(
            processar_xadrez_sincrono,
            df,
            view_mode
        )

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "xadrez",
        "view_mode": view_mode,
        "data_inicio_selecionada": data_inicio,
        "data_fim_selecionada": data_fim,
        "search_query": search_str,
        "error_message": error_message,
        "resumo_viagens": resumo_viagens,
        "dashboard_equipas": dashboard_equipas,
        # Variáveis vazias para o template não falhar
        "incentivo_tab": "motoristas",
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
        "metas": {}
    })