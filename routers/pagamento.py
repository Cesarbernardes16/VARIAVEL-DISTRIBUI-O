import datetime
import pandas as pd
import io
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa as funções de base de dados
from core.database import (
    get_dados_apurados, 
    get_cadastro_sincrono, 
    get_caixas_sincrono,
    get_indicadores_sincrono # <--- Importar esta
)
# Importa as funções de processamento que já criámos
from .incentivo import processar_incentivos_sincrono
from .caixas import processar_caixas_sincrono
# Importa a função que busca as metas
from .metas import _get_metas_sincrono

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def get_supabase(request: Request) -> Client:
    return request.state.supabase

# --- NOVA FUNÇÃO HELPER: BUSCAR TODOS OS DADOS ---
# (Para evitar repetir código nas duas rotas)
async def _get_dados_completos(data_inicio: str, data_fim: str, supabase: Client) -> Dict[str, Any]:
    """
    Busca todos os DataFrames necessários para os cálculos.
    """
    hoje = datetime.date.today()
    
    # Lógica de período de pagamento (copiada de incentivo.py)
    try:
        user_date_obj = datetime.date.fromisoformat(data_inicio)
        dia_corte = 26
        if user_date_obj.day < dia_corte:
            data_fim_periodo = user_date_obj.replace(day=25)
            data_inicio_periodo = (user_date_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=dia_corte)
        else:
            data_inicio_periodo = user_date_obj.replace(day=dia_corte)
            data_fim_periodo = (data_inicio_periodo + datetime.timedelta(days=32)).replace(day=25)
        data_inicio_periodo_str = data_inicio_periodo.isoformat()
        data_fim_periodo_str = data_fim_periodo.isoformat()
    except ValueError:
        data_inicio_periodo_str = data_inicio
        data_fim_periodo_str = data_fim

    # 1. Buscar Metas
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    
    # 2. Buscar Viagens (Tabela Distribuição)
    df_viagens, error_viagens = await run_in_threadpool(
        get_dados_apurados, supabase, data_inicio, data_fim, ""
    )
    
    # 3. Buscar Cadastro (Nomes, CPFs, Datas Admissão)
    df_cadastro, error_cadastro = await run_in_threadpool(get_cadastro_sincrono, supabase)
    
    # 4. Buscar Indicadores (KPIs)
    df_indicadores, error_kpis = await run_in_threadpool(
        get_indicadores_sincrono, supabase, data_inicio_periodo_str, data_fim_periodo_str
    )
    
    # 5. Buscar Caixas
    df_caixas, error_caixas = await run_in_threadpool(
        get_caixas_sincrono, supabase, data_inicio, data_fim
    )
    
    # Verifica o primeiro erro encontrado
    error_message = error_viagens or error_cadastro or error_kpis or error_caixas

    # IMPORTANTE: Remover duplicados de df_viagens (para Incentivo/Xadrez)
    # Criamos uma cópia para não afetar o cálculo de caixas
    df_viagens_dedup = None
    if df_viagens is not None:
        if 'MAPA' in df_viagens.columns:
            df_viagens_dedup = df_viagens.drop_duplicates(subset=['MAPA'])
        else:
            df_viagens_dedup = df_viagens.drop_duplicates()

    return {
        "metas": metas,
        "df_viagens_bruto": df_viagens, # Para Caixas
        "df_viagens_dedup": df_viagens_dedup, # Para KPIs
        "df_cadastro": df_cadastro,
        "df_indicadores": df_indicadores,
        "df_caixas": df_caixas,
        "error_message": error_message
    }

# --- NOVA FUNÇÃO HELPER: FUNDIR OS RESULTADOS ---
def _merge_resultados(
    motoristas_kpi: list, ajudantes_kpi: list,
    motoristas_caixas: list, ajudantes_caixas: list
) -> (pd.DataFrame, pd.DataFrame):
    
    # Colunas que queremos
    cols_kpi = ['cod', 'nome', 'cpf', 'total_premio']
    cols_caixas = ['cod', 'total_premio']

    # DataFrames de KPIs
    df_motoristas_kpi = pd.DataFrame(motoristas_kpi)[cols_kpi].rename(columns={"total_premio": "premio_kpi"})
    df_ajudantes_kpi = pd.DataFrame(ajudantes_kpi)[cols_kpi].rename(columns={"total_premio": "premio_kpi"})
    
    # DataFrames de Caixas
    df_motoristas_caixas = pd.DataFrame(motoristas_caixas)[cols_caixas].rename(columns={"total_premio": "premio_caixas"})
    df_ajudantes_caixas = pd.DataFrame(ajudantes_caixas)[cols_caixas].rename(columns={"total_premio": "premio_caixas"})

    # --- Merge Motoristas ---
    # Usamos 'outer' merge para incluir quem ganhou só KPI ou só Caixas
    df_motoristas_final = pd.merge(
        df_motoristas_kpi, 
        df_motoristas_caixas, 
        on='cod', 
        how='outer'
    )
    
    # --- Merge Ajudantes ---
    df_ajudantes_final = pd.merge(
        df_ajudantes_kpi,
        df_ajudantes_caixas,
        on='cod',
        how='outer'
    )
    
    # Preenche Nulos (quem não ganhou prémio) com 0 e soma o total
    for df in [df_motoristas_final, df_ajudantes_final]:
        df['premio_kpi'] = df['premio_kpi'].fillna(0)
        df['premio_caixas'] = df['premio_caixas'].fillna(0)
        df['total_a_pagar'] = df['premio_kpi'] + df['premio_caixas']
        # Remove quem não ganhou nada
        df = df[df['total_a_pagar'] > 0]

    return df_motoristas_final, df_ajudantes_final

# --- ROTA 1: Exibir Resumo no Ecrã ---
@router.get("/pagamento")
async def ler_relatorio_pagamento(
    request: Request, 
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    pagamento_tab: str = "motoristas", # Nova aba
    supabase: Client = Depends(get_supabase)
):
    hoje = datetime.date.today()
    data_inicio_filtro = data_inicio or hoje.replace(day=1).isoformat()
    data_fim_filtro = data_fim or hoje.isoformat()

    # 1. Buscar todos os dados
    dados = await _get_dados_completos(data_inicio_filtro, data_fim_filtro, supabase)
    
    # 2. Processar KPIs (Incentivo)
    # Usamos o df_viagens_dedup aqui
    motoristas_kpi, ajudantes_kpi = await run_in_threadpool(
        processar_incentivos_sincrono,
        dados["df_viagens_dedup"], dados["df_cadastro"], 
        dados["df_indicadores"], dados["metas"]
    )
    
    # 3. Processar Caixas
    # Usamos o df_viagens_bruto aqui
    motoristas_caixas, ajudantes_caixas = await run_in_threadpool(
        processar_caixas_sincrono,
        dados["df_viagens_bruto"], dados["df_cadastro"], 
        dados["df_caixas"], dados["metas"]
    )
    
    # 4. Fundir os resultados
    df_motoristas, df_ajudantes = _merge_resultados(
        motoristas_kpi, ajudantes_kpi,
        motoristas_caixas, ajudantes_caixas
    )

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "pagamento", # <-- Define a aba ativa
        "pagamento_tab": pagamento_tab, # <-- Aba de motorista/ajudante
        "data_inicio_selecionada": data_inicio_filtro,
        "data_fim_selecionada": data_fim_filtro,
        "error_message": dados["error_message"],
        "pagamento_motoristas": df_motoristas.to_dict('records'),
        "pagamento_ajudantes": df_ajudantes.to_dict('records'),
        
        # --- (Dados para o template não falhar) ---
        "metas": dados["metas"],
        "incentivo_tab": "motoristas",
        "caixas_tab": "motoristas",
        "view_mode": "equipas_fixas", 
        "search_query": "",
        "resumo_viagens": [],
        "dashboard_equipas": [],
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
        "caixas_motoristas": [],
        "caixas_ajudantes": [],
    })

# --- ROTA 2: Exportar Resumo para Excel ---
@router.get("/pagamento/exportar")
async def exportar_relatorio_pagamento(
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    supabase: Client = Depends(get_supabase)
):
    hoje = datetime.date.today()
    data_inicio_filtro = data_inicio or hoje.replace(day=1).isoformat()
    data_fim_filtro = data_fim or hoje.isoformat()

    # 1. Buscar todos os dados
    dados = await _get_dados_completos(data_inicio_filtro, data_fim_filtro, supabase)

    # 2. Processar KPIs (Incentivo)
    motoristas_kpi, ajudantes_kpi = await run_in_threadpool(
        processar_incentivos_sincrono,
        dados["df_viagens_dedup"], dados["df_cadastro"], 
        dados["df_indicadores"], dados["metas"]
    )
    
    # 3. Processar Caixas
    motoristas_caixas, ajudantes_caixas = await run_in_threadpool(
        processar_caixas_sincrono,
        dados["df_viagens_bruto"], dados["df_cadastro"], 
        dados["df_caixas"], dados["metas"]
    )
    
    # 4. Fundir os resultados
    df_motoristas, df_ajudantes = _merge_resultados(
        motoristas_kpi, ajudantes_kpi,
        motoristas_caixas, ajudantes_caixas
    )
    
    # 5. Gerar o Ficheiro Excel em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_motoristas.to_excel(writer, sheet_name='Motoristas', index=False)
        df_ajudantes.to_excel(writer, sheet_name='Ajudantes', index=False)
    
    output.seek(0)
    
    # Define o nome do ficheiro
    filename = f"Resumo_Pagamento_{data_inicio_filtro}_ate_{data_fim_filtro}.xlsx"
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )