import datetime
import pandas as pd
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool
from supabase import Client

# Importa a nossa lógica partilhada
# --- ALTERAÇÃO: Importa a nova função ---
from core.database import get_dados_apurados, get_cadastro_sincrono, get_indicadores_sincrono
from core.analysis import gerar_dashboard_e_mapas
from .metas import _get_metas_sincrono

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def get_supabase(request: Request) -> Client:
    return request.state.supabase

# Função de processamento síncrono (para o thread pool)
def processar_incentivos_sincrono(
    df_viagens: Optional[pd.DataFrame], 
    df_cadastro: Optional[pd.DataFrame], 
    df_indicadores: Optional[pd.DataFrame], # <-- DADOS REAIS
    metas: Dict[str, Any]
):
    
    incentivo_motoristas = []
    incentivo_ajudantes = []
    
    metas_motorista = metas.get("motorista", {})
    metas_ajudante = metas.get("ajudante", {})
    
    premio_motorista_map = {}
    default_premio_info = {
        "dev_pdv_val": "N/A", "dev_pdv_passou": False,
        "rating_val": "N/A", "rating_passou": False,
        "refugo_val": "N/A", "refugo_passou": False,
    }
    
    # --- ETAPA 1: Criar mapas de CPF e Indicadores ---
    cpf_motorista_map = {}
    cpf_ajudante_map = {}
    indicadores_map = {}

    if df_cadastro is not None and not df_cadastro.empty:
        # Mapa de Motoristas (Codigo_M -> CPF_M)
        df_motoristas_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_M'])].drop_duplicates(subset=['Codigo_M'])
        df_motoristas_cadastro['Codigo_M_int'] = pd.to_numeric(df_motoristas_cadastro['Codigo_M'], errors='coerce')
        df_motoristas_cadastro = df_motoristas_cadastro.dropna(subset=['Codigo_M_int'])
        df_motoristas_cadastro['Codigo_M_int'] = df_motoristas_cadastro['Codigo_M_int'].astype(int)
        cpf_motorista_map = df_motoristas_cadastro.set_index('Codigo_M_int')['CPF_M'].to_dict()

        # Mapa de Ajudantes (Codigo_J -> CPF_J)
        df_ajudantes_cadastro = df_cadastro[pd.notna(df_cadastro['Codigo_J'])].drop_duplicates(subset=['Codigo_J'])
        df_ajudantes_cadastro['Codigo_J_int'] = pd.to_numeric(df_ajudantes_cadastro['Codigo_J'], errors='coerce')
        df_ajudantes_cadastro = df_ajudantes_cadastro.dropna(subset=['Codigo_J_int'])
        df_ajudantes_cadastro['Codigo_J_int'] = df_ajudantes_cadastro['Codigo_J_int'].astype(int)
        cpf_ajudante_map = df_ajudantes_cadastro.set_index('Codigo_J_int')['CPF_J'].to_dict()

    if df_indicadores is not None and not df_indicadores.empty:
        # Mapa de Indicadores (Codigo_M -> Fila com resultados)
        # Converte para numérico (os dados já devem vir como ponto decimal, ex: 0.0414)
        df_indicadores['dev_pdv'] = pd.to_numeric(df_indicadores['dev_pdv'], errors='coerce')
        df_indicadores['Rating_tx'] = pd.to_numeric(df_indicadores['Rating_tx'], errors='coerce')
        df_indicadores['refugo'] = pd.to_numeric(df_indicadores['refugo'], errors='coerce')
        
        indicadores_map = df_indicadores.set_index('Codigo_M').to_dict('index')

    
    # --- ETAPA 2: Processar Motoristas e Ajudantes REAIS (do df_viagens) ---
    
    if df_viagens is not None and not df_viagens.empty:
        
        # --- LÓGICA DOS MOTORISTAS ---
        motoristas_no_periodo = df_viagens[['COD', 'MOTORISTA']].drop_duplicates(subset=['COD'])

        for _, motorista in motoristas_no_periodo.iterrows():
            linha = {}
            cod_motorista_int = int(motorista['COD'])
            
            linha["cpf"] = cpf_motorista_map.get(cod_motorista_int, "") 
            linha["cod"] = cod_motorista_int
            linha["nome"] = str(motorista.get('MOTORISTA', 'N/A')).strip()

            # --- DADOS DE PERFORMANCE REAIS ---
            indicadores_reais = indicadores_map.get(cod_motorista_int, {})
            
            # Converte de 0.0414 para 4.14 (multiplica por 100)
            dev_atingido = indicadores_reais.get('dev_pdv')
            if pd.notna(dev_atingido):
                dev_atingido = dev_atingido * 100 
            
            rating_atingido = indicadores_reais.get('Rating_tx')
            if pd.notna(rating_atingido):
                rating_atingido = rating_atingido * 100

            refugo_atingido = indicadores_reais.get('refugo')
            if pd.notna(refugo_atingido):
                refugo_atingido = refugo_atingido * 100
            # --- FIM DOS DADOS REAIS ---

            # O resto da lógica de cálculo de prémios continua
            dev_passou = (dev_atingido is not None and dev_atingido <= metas_motorista.get("dev_pdv_meta_perc", 0))
            linha["dev_pdv_val"] = f"{dev_atingido:.2f}%" if dev_atingido is not None else "N/A"
            linha["dev_pdv_premio_val"] = metas_motorista.get("dev_pdv_premio", 0) if dev_passou else 0.0
            
            rating_passou = (rating_atingido is not None and rating_atingido >= metas_motorista.get("rating_meta_perc", 0))
            linha["rating_val"] = f"{rating_atingido:.2f}%" if rating_atingido is not None else "N/A"
            linha["rating_premio_val"] = metas_motorista.get("rating_premio", 0) if rating_passou else 0.0

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

        # --- LÓGICA DOS AJUDANTES ---
        resultado_xadrez = gerar_dashboard_e_mapas(df_viagens)
        mapas = resultado_xadrez["mapas"]
        df_melted = resultado_xadrez["df_melted"]
        
        motorista_fixo_map = mapas.get("motorista_fixo_map", {})
        ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
        
        for _, ajudante in ajudantes_unicos.iterrows():
            cod_ajudante = ajudante['AJUDANTE_COD']
            nome_ajudante = ajudante['AJUDANTE_NOME']
            cod_motorista_fixo = motorista_fixo_map.get(cod_ajudante)
            
            performance_herdada = default_premio_info.copy()
            if cod_motorista_fixo:
                performance_herdada = premio_motorista_map.get(cod_motorista_fixo, default_premio_info)
            
            premio_dev_ajudante = metas_ajudante.get("dev_pdv_premio", 0) if performance_herdada["dev_pdv_passou"] else 0.0
            premio_rating_ajudante = metas_ajudante.get("rating_premio", 0) if performance_herdada["rating_passou"] else 0.0
            premio_refugo_ajudante = metas_ajudante.get("refugo_premio", 0) if performance_herdada["refugo_passou"] else 0.0
            
            ajudante_data = {
                "cpf": cpf_ajudante_map.get(cod_ajudante, ""), # <-- CPF REAL DO AJUDANTE
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
    
    incentivo_motoristas = sorted(incentivo_motoristas, key=lambda x: x['nome'])
        
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
    # As datas do filtro do utilizador (ex: 01/11 a 11/11)
    data_inicio_filtro = data_inicio or hoje.replace(day=1).isoformat()
    data_fim_filtro = data_fim or hoje.isoformat()
    
    incentivo_motoristas, incentivo_ajudantes = [], []
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)

    # --- INÍCIO DA NOVA LÓGICA DE PERÍODO ---
    try:
        # Usa a data de INÍCIO do filtro para descobrir o período
        user_date_obj = datetime.date.fromisoformat(data_inicio_filtro)
        dia_corte = 26
        
        if user_date_obj.day < dia_corte:
            # A data pertence ao período que termina este mês
            # Ex: user_date = 7 Nov -> período termina a 25 Nov
            data_fim_periodo = user_date_obj.replace(day=25)
            # O início foi no dia 26 do mês anterior
            data_inicio_periodo = (user_date_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=dia_corte)
        else:
            # A data pertence ao período que começa este mês
            # Ex: user_date = 27 Out -> período começa a 26 Out
            data_inicio_periodo = user_date_obj.replace(day=dia_corte)
            # O fim é no dia 25 do próximo mês
            data_fim_periodo = (data_inicio_periodo + datetime.timedelta(days=32)).replace(day=25)

        data_inicio_periodo_str = data_inicio_periodo.isoformat()
        data_fim_periodo_str = data_fim_periodo.isoformat()
    
    except ValueError:
        # Lida com datas inválidas
        error_message = "Formato de data inválido."
        data_inicio_periodo_str = data_inicio_filtro
        data_fim_periodo_str = data_fim_filtro
    # --- FIM DA NOVA LÓGICA DE PERÍODO ---


    # 1. Buscar dados de VIAGENS (usa o filtro do utilizador)
    df_viagens, error_message = await run_in_threadpool(
        get_dados_apurados,
        supabase,
        data_inicio_filtro,
        data_fim_filtro,
        search_str=""
    )
    
    # 2. Buscar dados de CADASTRO (CPFs)
    df_cadastro, error_cadastro = await run_in_threadpool(get_cadastro_sincrono, supabase)
    if error_cadastro and not error_message:
        error_message = error_cadastro
    
    # 3. Buscar dados de INDICADORES (Usa as datas do PERÍODO CALCULADO)
    df_indicadores, error_indicadores = await run_in_threadpool(
        get_indicadores_sincrono,
        supabase,
        data_inicio_periodo_str, # <-- Data calculada
        data_fim_periodo_str   # <-- Data calculada
    )
    if error_indicadores and not error_message:
        error_message = error_indicadores
    
    # Remove duplicatas do DataFrame de viagens
    if error_message is None and df_viagens is not None:
        if 'MAPA' in df_viagens.columns:
            df_viagens = df_viagens.drop_duplicates(subset=['MAPA'])
        else:
            df_viagens = df_viagens.drop_duplicates()
    
    # 4. Processar incentivos
    if error_message is None:
        incentivo_motoristas, incentivo_ajudantes = await run_in_threadpool(
            processar_incentivos_sincrono,
            df_viagens,
            df_cadastro,
            df_indicadores, 
            metas
        )
    else:
        # Tenta processar mesmo com erro (pode mostrar listas vazias)
        incentivo_motoristas, incentivo_ajudantes = await run_in_threadpool(
            processar_incentivos_sincrono,
            df_viagens,
            df_cadastro,
            df_indicadores,
            metas
        )


    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "incentivo",
        "incentivo_tab": incentivo_tab,
        "data_inicio_selecionada": data_inicio_filtro, # Mostra o filtro do utilizador
        "data_fim_selecionada": data_fim_filtro,       # Mostra o filtro do utilizador
        "error_message": error_message,
        "incentivo_motoristas": incentivo_motoristas,
        "incentivo_ajudantes": incentivo_ajudantes,
        "metas": metas,
        "view_mode": "equipas_fixas", 
        "search_query": "",
        "resumo_viagens": [],
        "dashboard_equipas": [],
    })