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
# --- ALTERAÇÃO AQUI ---
# Importa a função com o nome correto
from .metas import _get_metas_sincrono
# --- FIM DA ALTERAÇÃO ---

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Função para obter o cliente Supabase do estado da request
def get_supabase(request: Request) -> Client:
    return request.state.supabase

# Função de processamento síncrono (para o thread pool)
def processar_incentivos_sincrono(df: Optional[pd.DataFrame], metas: Dict[str, Any]):
    
    incentivo_motoristas = []
    incentivo_ajudantes = []
    
    # Pega as metas específicas
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    
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
        "dev_pdv_val": "N/A", "dev_pdv_passou": False,
        "rating_val": "N/A", "rating_passou": False,
        "refugo_val": "N/A", "refugo_passou": False,
    }

    for motorista in dados_motoristas:
        linha = motorista.copy()
        
        # Lógica de Devolução (Motorista)
        dev_atingido = linha.get("dev_pdv")
        dev_passou = (dev_atingido is not None and dev_atingido <= metas_motorista.get("dev_pdv_meta_perc", 0))
        linha["dev_pdv_val"] = f"{dev_atingido:.2f}%" if dev_atingido is not None else "N/A"
        linha["dev_pdv_premio_val"] = metas_motorista.get("dev_pdv_premio", 0) if dev_passou else 0.0
        
        # Lógica de Rating (Motorista)
        rating_atingido = linha.get("rating")
        rating_passou = (rating_atingido is not None and rating_atingido >= metas_motorista.get("rating_meta_perc", 0))
        linha["rating_val"] = f"{rating_atingido:.2f}%" if rating_atingido is not None else "N/A"
        linha["rating_premio_val"] = metas_motorista.get("rating_premio", 0) if rating_passou else 0.0

        # Lógica de Refugo (Motorista)
        refugo_atingido = linha.get("refugo")
        refugo_passou = (refugo_atingido is not None and refugo_atingido <= metas_motorista.get("refugo_meta_perc", 0))
        linha["refugo_val"] = f"{refugo_atingido:.2f}%" if refugo_atingido is not None else "N/A"
        linha["refugo_premio_val"] = metas_motorista.get("refugo_premio", 0) if refugo_passou else 0.0

        linha["total_premio"] = linha["dev_pdv_premio_val"] + linha["rating_premio_val"] + linha["refugo_premio_val"]
        incentivo_motoristas.append(linha)
        
        # Salva o RESULTADO (pass/fail) e os VALORES ATINGIDOS no mapa
        if linha["cod"]:
            premio_motorista_map[linha["cod"]] = {
                "dev_pdv_val": linha["dev_pdv_val"], "dev_pdv_passou": dev_passou,
                "rating_val": linha["rating_val"], "rating_passou": rating_passou,
                "refugo_val": linha["refugo_val"], "refugo_passou": refugo_passou,
            }

    # --- ETAPA 2: Ligar Ajudantes (só se tivermos dados reais) ---
    if df is not None and not df.empty:
        # O 'df' que recebemos aqui já foi limpo (sem duplicatas)
        resultado_xadrez = gerar_dashboard_e_mapas(df)
        mapas = resultado_xadrez["mapas"]
        df_melted = resultado_xadrez["df_melted"]
        
        motorista_fixo_map = mapas.get("motorista_fixo_map", {})
        
        ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
        
        for _, ajudante in ajudantes_unicos.iterrows():
            cod_ajudante = ajudante['AJUDANTE_COD']
            nome_ajudante = ajudante['AJUDANTE_NOME']
            cod_motorista_fixo = motorista_fixo_map.get(cod_ajudante)
            
            # Pega o resultado (pass/fail) do motorista fixo
            performance_herdada = default_premio_info.copy()
            if cod_motorista_fixo:
                performance_herdada = premio_motorista_map.get(cod_motorista_fixo, default_premio_info)
            
            # --- ALTERAÇÃO: Calcula o prémio do Ajudante ---
            # O ajudante herda o "pass/fail", mas o prémio é da tabela de ajudantes
            
            premio_dev_ajudante = metas_ajudante.get("dev_pdv_premio", 0) if performance_herdada["dev_pdv_passou"] else 0.0
            premio_rating_ajudante = metas_ajudante.get("rating_premio", 0) if performance_herdada["rating_passou"] else 0.0
            premio_refugo_ajudante = metas_ajudante.get("refugo_premio", 0) if performance_herdada["refugo_passou"] else 0.0
            
            ajudante_data = {
                "cpf": "", # CPF em branco
                "cod": cod_ajudante,
                "nome": nome_ajudante,
                
                "dev_pdv_val": performance_herdada["dev_pdv_val"],
                "dev_pdv_premio_val": premio_dev_ajudante,
                
                "rating_val": performance_herdada["rating_val"],
                "rating_premio_val": premio_rating_ajudante,
                
                "refugo_val": performance_herdada["refugo_val"],
                "refugo_premio_val": premio_refugo_ajudante,
                
                "total_premio": premio_dev_ajudante + premio_rating_ajudante + premio_refugo_ajudante
            }
            
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
    
    # --- ALTERAÇÃO AQUI ---
    # Busca as metas usando a função correta no threadpool
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    # --- FIM DA ALTERAÇÃO ---

    df, error_message = await run_in_threadpool(
        get_dados_apurados,
        supabase,
        data_inicio,
        data_fim,
        search_str=""
    )
    
    # Remove duplicatas do DataFrame principal
    if error_message is None and df is not None:
        if 'MAPA' in df.columns:
            df = df.drop_duplicates(subset=['MAPA'])
        else:
            df = df.drop_duplicates()
    
    # 2. Processar incentivos (em thread pool)
    incentivo_motoristas, incentivo_ajudantes = await run_in_threadpool(
        processar_incentivos_sincrono,
        df,
        metas # Passa o dicionário de metas aninhado
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
        "metas": metas, # Passa as metas aninhadas
        "view_mode": "equipas_fixas", 
        "search_query": "",
        "resumo_viagens": [],
        "dashboard_equipas": [],
    })