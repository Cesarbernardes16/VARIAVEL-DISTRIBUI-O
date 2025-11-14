import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa as funções de base de dados
from core.database import (
    get_dados_apurados, 
    get_cadastro_sincrono, 
    get_caixas_sincrono
)
# Importa a função que busca as metas
from .metas import _get_metas_sincrono

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def get_supabase(request: Request) -> Client:
    return request.state.supabase

# --- Função Helper para a Regra de Antiguidade (Inalterada) ---
def _get_valor_por_caixa(dias_antiguidade: int, metas_colaborador: Dict[str, Any]) -> float:
    """
    Retorna o valor (R$) por caixa com base nos dias de antiguidade.
    """
    try:
        # Verifica do maior para o menor
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n3", 1825):
             return metas_colaborador.get("meta_cx_valor_n4", 0.0)
        
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n2", 730):
            return metas_colaborador.get("meta_cx_valor_n3", 0.0)
            
        if dias_antiguidade > metas_colaborador.get("meta_cx_dias_n1", 365):
            return metas_colaborador.get("meta_cx_valor_n2", 0.0)
            
        # Nível 1 (Default)
        return metas_colaborador.get("meta_cx_valor_n1", 0.0)
        
    except Exception:
        return 0.0

# --- Função Principal de Processamento (Inalterada) ---
def processar_caixas_sincrono(
    df_viagens: Optional[pd.DataFrame], 
    df_cadastro: Optional[pd.DataFrame], 
    df_caixas: Optional[pd.DataFrame], 
    metas: Dict[str, Any]
):
    
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    hoje = datetime.date.today()
    
    # --- 1. Criar Mapa de Antiguidade (Motoristas) ---
    motorista_antiguidade_map = {}
    motorista_info_map = {} 
    
    if df_cadastro is not None:
        df_motoristas_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_M'])].drop_duplicates(subset=['Codigo_M'])
        df_motoristas_cadastro['Codigo_M_int'] = pd.to_numeric(df_motoristas_cadastro['Codigo_M'], errors='coerce').fillna(0).astype(int)
        df_motoristas_cadastro['Data_M_dt'] = pd.to_datetime(df_motoristas_cadastro['Data_M'], errors='coerce').dt.date

        for _, row in df_motoristas_cadastro.iterrows():
            cod = row['Codigo_M_int']
            if cod == 0: continue
            data_inicio = row['Data_M_dt']
            dias = (hoje - data_inicio).days if data_inicio and pd.notna(data_inicio) else 0
            motorista_antiguidade_map[cod] = dias
            motorista_info_map[cod] = {
                "nome": str(row.get('Nome_M', '')).strip(),
                "cpf": str(row.get('CPF_M', '')).strip()
            }

    # --- 2. Criar Mapa de Antiguidade (Ajudantes) ---
    ajudante_antiguidade_map = {}
    ajudante_info_map = {}
    
    if df_cadastro is not None:
        df_ajudantes_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_J'])].drop_duplicates(subset=['Codigo_J'])
        df_ajudantes_cadastro['Codigo_J_int'] = pd.to_numeric(df_ajudantes_cadastro['Codigo_J'], errors='coerce').fillna(0).astype(int)
        df_ajudantes_cadastro['Data_J_dt'] = pd.to_datetime(df_ajudantes_cadastro['Data_J'], errors='coerce').dt.date

        for _, row in df_ajudantes_cadastro.iterrows():
            cod = row['Codigo_J_int']
            if cod == 0: continue
            data_inicio = row['Data_J_dt']
            dias = (hoje - data_inicio).days if data_inicio and pd.notna(data_inicio) else 0
            ajudante_antiguidade_map[cod] = dias
            ajudante_info_map[cod] = {
                "nome": str(row.get('Nome_J', '')).strip(),
                "cpf": str(row.get('CPF_J', '')).strip()
            }

    # --- 3. Criar Mapa de Caixas ---
    mapa_caixas_total = {}
    if df_caixas is not None and not df_caixas.empty:
        df_caixas_limpo = df_caixas.drop_duplicates(subset=['mapa'])
        mapa_caixas_total = df_caixas_limpo.set_index('mapa')['caixas'].to_dict()

    # --- 4. Acumular Caixas por Colaborador ---
    motorista_caixas_acumuladas = {}
    ajudante_caixas_acumuladas = {}
    
    colunas_ajudantes = [col for col in df_viagens.columns if col.startswith('CODJ_')]

    if df_viagens is not None:
        for _, viagem in df_viagens.iterrows():
            mapa_id = str(viagem.get('MAPA', ''))
            caixas_do_mapa = float(mapa_caixas_total.get(mapa_id, 0))
            
            if caixas_do_mapa == 0:
                continue 
            
            # Processa Motorista
            cod_motorista = int(viagem.get('COD', 0))
            if cod_motorista in motorista_info_map: 
                motorista_caixas_acumuladas[cod_motorista] = motorista_caixas_acumuladas.get(cod_motorista, 0) + caixas_do_mapa
                
            # Processa Ajudantes
            for col in colunas_ajudantes:
                cod_ajudante = pd.to_numeric(viagem.get(col), errors='coerce')
                if cod_ajudante and pd.notna(cod_ajudante):
                    cod_ajudante_int = int(cod_ajudante)
                    if cod_ajudante_int in ajudante_info_map: 
                        ajudante_caixas_acumuladas[cod_ajudante_int] = ajudante_caixas_acumuladas.get(cod_ajudante_int, 0) + caixas_do_mapa

    # --- 5. Montar Resultados Finais ---
    resultado_motoristas = []
    for cod, total_caixas in motorista_caixas_acumuladas.items():
        if total_caixas == 0:
            continue
            
        info = motorista_info_map.get(cod, {"cpf": "N/A", "nome": f"COD {cod}"})
        dias = motorista_antiguidade_map.get(cod, 0)
        valor_cx = _get_valor_por_caixa(dias, metas_motorista)
        total_bonus = total_caixas * valor_cx
        
        resultado_motoristas.append({
            "cpf": info["cpf"],
            "cod": cod,
            "nome": info["nome"],
            "total_caixas": total_caixas,
            "valor_por_caixa": valor_cx,
            "total_premio": total_bonus
        })

    resultado_ajudantes = []
    for cod, total_caixas in ajudante_caixas_acumuladas.items():
        if total_caixas == 0:
            continue

        info = ajudante_info_map.get(cod, {"cpf": "N/A", "nome": f"COD {cod}"})
        dias = ajudante_antiguidade_map.get(cod, 0)
        valor_cx = _get_valor_por_caixa(dias, metas_ajudante)
        total_bonus = total_caixas * valor_cx
        
        resultado_ajudantes.append({
            "cpf": info["cpf"],
            "cod": cod,
            "nome": info["nome"],
            "total_caixas": total_caixas,
            "valor_por_caixa": valor_cx,
            "total_premio": total_bonus
        })

    # Ordenar por nome
    resultado_motoristas = sorted(resultado_motoristas, key=lambda x: x['nome'])
    resultado_ajudantes = sorted(resultado_ajudantes, key=lambda x: x['nome'])
    
    return resultado_motoristas, resultado_ajudantes


# --- A Rota FastAPI (Endpoint) ---
@router.get("/caixas")
async def ler_relatorio_caixas(
    request: Request, 
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    caixas_tab: str = "motoristas", # <-- NOVO PARÂMETRO
    supabase: Client = Depends(get_supabase)
):
    
    hoje = datetime.date.today()
    data_inicio_filtro = data_inicio or hoje.replace(day=1).isoformat()
    data_fim_filtro = data_fim or hoje.isoformat()
    
    # --- 1. Buscar Metas (já inclui as metas de caixas) ---
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)
    
    # --- 2. Buscar Viagens (Quem trabalhou em que mapa) ---
    df_viagens, error_message = await run_in_threadpool(
        get_dados_apurados,
        supabase,
        data_inicio_filtro,
        data_fim_filtro,
        search_str="" 
    )
    
    # --- 3. Buscar Cadastro (Para Antiguidade e Nomes) ---
    df_cadastro, error_cadastro = await run_in_threadpool(get_cadastro_sincrono, supabase)
    if error_cadastro and not error_message:
        error_message = error_cadastro
        
    # --- 4. Buscar Caixas (Tabela 'Caixas') ---
    df_caixas, error_caixas = await run_in_threadpool(
        get_caixas_sincrono,
        supabase,
        data_inicio_filtro,
        data_fim_filtro
    )
    if error_caixas and not error_message:
        error_message = error_caixas
            
    # --- 5. Processar os dados ---
    resultado_motoristas, resultado_ajudantes = [], []
    if error_message is None:
        resultado_motoristas, resultado_ajudantes = await run_in_threadpool(
            processar_caixas_sincrono,
            df_viagens,
            df_cadastro,
            df_caixas,
            metas
        )

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "caixas",
        "caixas_tab": caixas_tab, # <-- PASSA A NOVA VARIÁVEL
        "data_inicio_selecionada": data_inicio_filtro,
        "data_fim_selecionada": data_fim_filtro,
        "error_message": error_message,
        "caixas_motoristas": resultado_motoristas, 
        "caixas_ajudantes": resultado_ajudantes,   
        
        "metas": metas,
        "incentivo_tab": "motoristas", # <-- Valor default para o template
        "view_mode": "equipas_fixas", 
        "search_query": "",
        "resumo_viagens": [],
        "dashboard_equipas": [],
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
    })