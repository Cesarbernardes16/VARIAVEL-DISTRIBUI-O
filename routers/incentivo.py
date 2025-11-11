import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa a nossa lógica partilhada
from core.database import get_dados_apurados
from core.analysis import gerar_dashboard_e_mapas
from .metas import _get_metas # Importa as metas do novo router

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Função para obter o cliente Supabase do estado da request
def get_supabase(request: Request) -> Client:
    return request.state.supabase

# Função de processamento síncrono (para o thread pool)
def processar_incentivos_sincrono(df: Optional[pd.DataFrame], metas: Dict[str, Any]):
    
    incentivo_motoristas = []
    incentivo_ajudantes = []
    
    # --- ETAPA 1: Gerar dados FALSOS (MOCK) para Motoristas ---
    dados_motoristas = [
        {"cpf": "Não Aparece", "cod": 9999, "nome": "ADEMILSON PANTALEAO (AFASTADO)"},
        {"cpf": "21676141871", "cod": 248, "nome": "ADOLFO RAMOS DA SILVA", "dev_pdv": 4.90, "rating": 20.98, "refugo": None},
        {"cpf": "92186866153", "cod": 7, "nome": "ALAN CORREIA DOS SANTOS CARDEN", "dev_pdv": 8.00, "rating": 15.20, "refugo": None},
        {"cpf": "93303068100", "cod": 10, "nome": "ALEXANDER DOS SANTOS COSTA", "dev_pdv": 1.18, "rating": 37.11, "refugo": 0.5},
        {"cpf": "7600204185", "cod": 192, "nome": "ALEXANDRE AQUINO CACERES", "dev_pdv": 1.21, "rating": 58.18, "refugo": None},
        {"cpf": "5224088186", "cod": 665, "nome": "ALEXANDRE DOS SANTOS COSTA", "dev_pdv": 0.98, "rating": 25.24, "refugo": None},
    ]
    
    premio_motorista_map = {}
    default_premio_info = {
        "dev_pdv_val": "N/A", "dev_pdv_premio_val": 0.0,
        "rating_val": "N/A", "rating_premio_val": 0.0,
        "refugo_val": "N/A", "refugo_premio_val": 0.0,
        "total_premio": 0.0
    }

    for motorista in dados_motoristas:
        linha = motorista.copy()
        dev_atingido = linha.get("dev_pdv")
        linha["dev_pdv_val"] = f"{dev_atingido:.2f}%" if dev_atingido is not None else "N/A"
        linha["dev_pdv_premio_val"] = metas["dev_pdv_premio"] if (dev_atingido is not None and dev_atingido <= metas["dev_pdv_meta_perc"]) else 0.0
        
        rating_atingido = linha.get("rating")
        linha["rating_val"] = f"{rating_atingido:.2f}%" if rating_atingido is not None else "N/A"
        linha["rating_premio_val"] = metas["rating_premio"] if (rating_atingido is not None and rating_atingido >= metas["rating_meta_perc"]) else 0.0

        refugo_atingido = linha.get("refugo")
        linha["refugo_val"] = f"{refugo_atingido:.2f}%" if refugo_atingido is not None else "N/A"
        linha["refugo_premio_val"] = metas["refugo_premio"] if (refugo_atingido is not None and refugo_atingido <= metas["refugo_meta_perc"]) else 0.0

        linha["total_premio"] = linha["dev_pdv_premio_val"] + linha["rating_premio_val"] + linha["refugo_premio_val"]
        incentivo_motoristas.append(linha)
        
        if linha["cod"]:
            premio_motorista_map[linha["cod"]] = {
                "dev_pdv_val": linha["dev_pdv_val"],
                "dev_pdv_premio_val": linha["dev_pdv_premio_val"],
                "rating_val": linha["rating_val"],
                "rating_premio_val": linha["rating_premio_val"],
                "refugo_val": linha["refugo_val"],
                "refugo_premio_val": linha["refugo_premio_val"],
                "total_premio": linha["total_premio"]
            }

    # --- ETAPA 2: Ligar Ajudantes (só se tivermos dados reais) ---
    if df is not None and not df.empty:
        resultado_xadrez = gerar_dashboard_e_mapas(df)
        mapas = resultado_xadrez["mapas"]
        df_melted = resultado_xadrez["df_melted"]
        
        motorista_fixo_map = mapas.get("motorista_fixo_map", {})
        
        ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
        
        for _, ajudante in ajudantes_unicos.iterrows():
            cod_ajudante = ajudante['AJUDANTE_COD']
            nome_ajudante = ajudante['AJUDANTE_NOME']
            cod_motorista_fixo = motorista_fixo_map.get(cod_ajudante)
            
            premio_info_herdado = default_premio_info.copy()
            if cod_motorista_fixo:
                premio_info_herdado = premio_motorista_map.get(cod_motorista_fixo, default_premio_info)
            
            ajudante_data = {
                "cpf": "", # CPF em branco
                "cod": cod_ajudante,
                "nome": nome_ajudante,
            }
            
            ajudante_data.update(premio_info_herdado)
            incentivo_ajudantes.append(ajudante_data)
            
        incentivo_ajudantes = sorted(incentivo_ajudantes, key=lambda x: x['nome'])
        
    return incentivo_motoristas, incentivo_ajudantes

@router.get("/incentivo")
async def ler_relatorio_incentivo(
    request: Request, 
    incentivo_tab: str = "motoristas",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    supabase: Client = Depends(get_supabase)
):
    
    hoje = datetime.date.today()
    data_inicio = data_inicio or hoje.replace(day=1).isoformat()
    data_fim = data_fim or hoje.isoformat()
    
    incentivo_motoristas, incentivo_ajudantes = [], []
    metas = _get_metas() # Pega as metas

    # --- ALTERAÇÃO: Chamar get_dados_apurados (síncrono) no threadpool ---
    df, error_message = await run_in_threadpool(
        get_dados_apurados,
        supabase,
        data_inicio,
        data_fim,
        search_str=""
    )
    
    # 2. Processar incentivos (em thread pool)
    # Passamos o 'df' (mesmo que seja None)
    incentivo_motoristas, incentivo_ajudantes = await run_in_threadpool(
        processar_incentivos_sincrono,
        df,
        metas
    )

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "incentivo",
        "incentivo_tab": incentivo_tab,
        "data_inicio_selecionada": data_inicio,
        "data_fim_selecionada": data_fim,
        "error_message": error_message,
        "incentivo_motoristas": incentivo_motoristas,
        "incentivo_ajudantes": incentivo_ajudantes,
        "metas": metas,
        # Variáveis vazias para o template não falhar
        "view_mode": "equipas_fixas", 
        "search_query": "",
        "resumo_viagens": [],
        "dashboard_equipas": [],
    })