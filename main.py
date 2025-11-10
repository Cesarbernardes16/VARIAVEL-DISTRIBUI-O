import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from supabase import create_client, Client
import datetime
import calendar
from typing import Optional, List, Dict, Any
import unicodedata
from fastapi.concurrency import run_in_threadpool

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)
NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

# --- FUNÇÃO DE LIMPEZA DE TEXTO (Inalterada) ---
def limpar_texto(text):
    if not isinstance(text, str):
        return text
    text_upper = text.upper()
    nfkd_form = unicodedata.normalize('NFKD', text_upper)
    ascii_bytes = nfkd_form.encode('ASCII', 'ignore')
    return ascii_bytes.decode('utf-8')

# --- LÓGICA DE ANÁLISE (Funções Auxiliares Inalteradas) ---

def _preparar_dataframe_ajudantes(df: pd.DataFrame) -> pd.DataFrame:
    ajudantes_dfs = []
    colunas_ajudante = sorted(df.filter(regex=r'^AJUDANTE_\d+$').columns)

    for aj_col in colunas_ajudante:
        num = aj_col.split('_')[-1]
        cod_col = f'CODJ_{num}'
        
        if cod_col in df.columns:
            temp_df = df[['COD', aj_col, cod_col]].copy()
            temp_df.rename(columns={
                'COD': 'MOTORISTA_COD', 
                aj_col: 'AJUDANTE_NOME', 
                cod_col: 'AJUDANTE_COD'
            }, inplace=True)
            temp_df['POSICAO'] = f'AJUDANTE {num}'
            ajudantes_dfs.append(temp_df)
            
    if not ajudantes_dfs:
        return pd.DataFrame(columns=['MOTORISTA_COD', 'AJUDANTE_NOME', 'AJUDANTE_COD', 'POSICAO'])

    df_global_melted = pd.concat(ajudantes_dfs)
    df_global_melted.dropna(subset=['AJUDANTE_NOME'], inplace=True)
    df_global_melted = df_global_melted[df_global_melted['AJUDANTE_NOME'].str.strip() != '']
    df_global_melted['AJUDANTE_COD'] = pd.to_numeric(df_global_melted['AJUDANTE_COD'], errors='coerce')
    df_global_melted.dropna(subset=['AJUDANTE_COD'], inplace=True)
    df_global_melted['AJUDANTE_COD'] = df_global_melted['AJUDANTE_COD'].astype(int)
    
    return df_global_melted

def _calcular_mapas_referencia(df_melted: pd.DataFrame, df_original: pd.DataFrame) -> dict:
    motorista_fixo_map = df_melted.groupby('AJUDANTE_COD')['MOTORISTA_COD'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    ).to_dict()
    posicao_fixa_map = df_melted.groupby('AJUDANTE_COD')['POSICAO'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else 'AJUDANTE 1'
    ).to_dict()
    nome_ajudante_map = df_melted.groupby('AJUDANTE_COD')['AJUDANTE_NOME'].apply(
        lambda x: x.mode().iloc[0] if not x.mode().empty else ''
    ).to_dict()
    contagem_viagens_motorista = df_original['COD'].value_counts().to_dict()
    
    return {
        "motorista_fixo_map": motorista_fixo_map,
        "posicao_fixa_map": posicao_fixa_map,
        "nome_ajudante_map": nome_ajudante_map,
        "contagem_viagens_motorista": contagem_viagens_motorista
    }

def _classificar_e_atribuir_viagens(
    info_linha: Dict[str, Any], 
    viagens_com_motorista: pd.DataFrame, 
    mapas: Dict[str, Any], 
    total_viagens: int,
    regras: Dict[str, Any]
):
    viagens_fixas = []
    viagens_visitantes = []
    
    for _, viagem in viagens_com_motorista.iterrows():
        viagem_data = {
            'cod_ajudante': int(viagem['AJUDANTE_COD']),
            'nome_ajudante': viagem['AJUDANTE_NOME'],
            'num_viagens': viagem['VIAGENS']
        }
        is_primary_fixed = mapas["motorista_fixo_map"].get(viagem_data['cod_ajudante']) == info_linha['COD']
        significance_ratio = (viagem_data['num_viagens'] / total_viagens) if total_viagens > 0 else 0
        is_significant = significance_ratio > regras["RATIO_SIGNIFICANCIA_FIXO"]

        if is_primary_fixed or is_significant:
            viagem_data['posicao_fixa'] = mapas["posicao_fixa_map"].get(viagem_data['cod_ajudante'], 'AJUDANTE 1')
            viagens_fixas.append(viagem_data)
        else:
            viagens_visitantes.append(viagem_data)

    tem_fixo_acima_de_10 = False
    for fixo in viagens_fixas:
        if fixo['num_viagens'] > regras["MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA"]:
            tem_fixo_acima_de_10 = True
        posicao_str = fixo['posicao_fixa'].replace(' ', '_')
        cod_posicao_str = f"CODJ_{posicao_str.split('_')[-1]}"
        info_linha[posicao_str] = f"{fixo['nome_ajudante'].strip()} ({fixo['num_viagens']})"
        info_linha[cod_posicao_str] = fixo['cod_ajudante']

    condicao_motorista = total_viagens > regras["MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA"]
    limite_minimo_visitante = regras["LIMITE_VISITANTE_PADRAO"]
    if condicao_motorista and tem_fixo_acima_de_10:
        limite_minimo_visitante = regras["LIMITE_VISITANTE_ESTRITO"]

    for visitante in viagens_visitantes:
        if visitante['num_viagens'] > limite_minimo_visitante:
            info_linha['VISITANTES'].append(f"{visitante['nome_ajudante'].strip()} ({visitante['num_viagens']}x)")

def gerar_dashboard_equipas_fixas(df: pd.DataFrame) -> List[Dict[str, Any]]:
    regras = {
        "RATIO_SIGNIFICANCIA_FIXO": 0.40,
        "MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA": 10,
        "MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA": 15,
        "LIMITE_VISITANTE_ESTRITO": 2,
        "LIMITE_VISITANTE_PADRAO": 1,
    }
    df_melted = _preparar_dataframe_ajudantes(df)
    if df_melted.empty:
        return []

    mapas = _calcular_mapas_referencia(df_melted, df)
    
    contagem_viagens_ajudantes = df_melted.groupby(['MOTORISTA_COD', 'AJUDANTE_COD']).size().reset_index(name='VIAGENS')
    contagem_viagens_ajudantes['AJUDANTE_NOME'] = contagem_viagens_ajudantes['AJUDANTE_COD'].map(mapas["nome_ajudante_map"])
    
    dashboard_data = []
    colunas_motorista_base = ['COD', 'MOTORISTA', 'MOTORISTA_2', 'COD_2']
    colunas_existentes = [col for col in colunas_motorista_base if col in df.columns]
    motoristas_no_periodo = df[colunas_existentes].drop_duplicates(subset=['COD'])
    
    for _, motorista_row in motoristas_no_periodo.iterrows():
        cod_motorista = int(motorista_row['COD'])
        total_viagens = mapas["contagem_viagens_motorista"].get(cod_motorista, 0)
        
        nome_motorista = motorista_row.get('MOTORISTA')
        nome_formatado = f"COD: {cod_motorista} ({total_viagens})" if pd.isna(nome_motorista) or str(nome_motorista).strip() == '' else f"{nome_motorista} ({total_viagens})"
        
        info_linha = {
            'MOTORISTA': nome_formatado, 'COD': cod_motorista,
            'MOTORISTA_2': motorista_row.get('MOTORISTA_2'), 'COD_2': motorista_row.get('COD_2'),
            'VISITANTES': []
        }
        
        max_pos = df_melted['POSICAO'].nunique() if not df_melted.empty else 3 # Default 3 se vazio
        for i in range(1, max_pos + 1):
            info_linha[f'AJUDANTE_{i}'] = ''
            info_linha[f'CODJ_{i}'] = ''
        
        viagens_com_motorista = contagem_viagens_ajudantes[contagem_viagens_ajudantes['MOTORISTA_COD'] == cod_motorista]
        
        _classificar_e_atribuir_viagens(
            info_linha, viagens_com_motorista, mapas, total_viagens, regras
        )
        dashboard_data.append(info_linha)
    
    for linha in dashboard_data:
        for key, value in linha.items():
            if value is None:
                linha[key] = ''
        
    return sorted(dashboard_data, key=lambda x: x.get('MOTORISTA') or '')


# --- ALTERAÇÃO: Função Síncrona atualizada ---
def processar_dados_sincrono(
    data_inicio_str: str, 
    data_fim_str: str, 
    search_str: str, 
    view_mode: str
):
    """
    Função bloqueante atualizada para usar data_inicio/data_fim e search_str.
    """
    df = pd.DataFrame()
    resumo_viagens, dashboard_equipas = [], None
    error_message = None

    try:
        # Busca paginada de dados (Bloqueante)
        dados_completos = []
        page_size = 1000
        page = 0
        while True:
            query = (
                supabase.table(NOME_DA_TABELA)
                .select("*")
                .gte(NOME_COLUNA_DATA, data_inicio_str) # <-- ALTERAÇÃO
                .lte(NOME_COLUNA_DATA, data_fim_str)   # <-- ALTERAÇÃO
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute()
            
            if not response.data:
                break
            dados_completos.extend(response.data)
            page += 1
            if len(response.data) < page_size:
                break
        
        if not dados_completos:
            error_message = "Nenhum dado encontrado para o período selecionado."
        else:
            df = pd.DataFrame(dados_completos)

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase: {e}")
        error_message = "Erro ao conectar ao banco de dados. Tente novamente mais tarde."
        return resumo_viagens, dashboard_equipas, error_message

    if not df.empty:
        # 1. Limpeza de Texto (Igual a antes)
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].apply(limpar_texto)
        
        if 'COD' in df.columns:
            df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
            df.dropna(subset=['COD'], inplace=True)
            df['COD'] = df['COD'].astype(int)
        else:
             df = pd.DataFrame() 
             if not error_message:
                 error_message = "A coluna 'COD' principal não foi encontrada nos dados."

    # --- ALTERAÇÃO: Lógica de Filtro de Pesquisa ---
    if search_str and not df.empty:
        search_clean = limpar_texto(search_str)
        
        # Define as colunas onde a pesquisa será aplicada
        colunas_busca = [
            'MOTORISTA', 'MOTORISTA_2', 
            'AJUDANTE_1', 'AJUDANTE_2', 'AJUDANTE_3'
        ]
        # Garante que só vamos pesquisar em colunas que existem no DataFrame
        colunas_existentes_busca = [col for col in colunas_busca if col in df.columns]
        
        # Cria uma máscara booleana (inicialmente toda False)
        mask = pd.Series(False, index=df.index)
        for col in colunas_existentes_busca:
            # Usa .str.contains() para o filtro "like"
            # | (pipe) significa "OU"
            mask = mask | df[col].str.contains(search_clean, na=False)
            
        df = df[mask] # Aplica o filtro ao DataFrame
        
        if df.empty and not error_message:
            error_message = f"Nenhum dado encontrado para o termo de busca: '{search_str}'"
    # --- FIM DA ALTERAÇÃO ---

    if not df.empty:
        # 3. Análise de Dados (Igual a antes)
        if view_mode == 'equipas_fixas':
            dashboard_equipas = gerar_dashboard_equipas_fixas(df)
        else: 
            colunas_resumo = ['MAPA', 'MOTORISTA', 'COD', 'MOTORISTA_2', 'COD_2', 'AJUDANTE_1', 'CODJ_1', 'AJUDANTE_2', 'CODJ_2', 'AJUDANTE_3', 'CODJ_3']
            colunas_existentes = [col for col in colunas_resumo if col in df.columns]
            resumo_df = df[colunas_existentes].sort_values(by='MOTORISTA' if 'MOTORISTA' in colunas_existentes else colunas_existentes[0])
            resumo_df.fillna('', inplace=True)
            resumo_viagens = resumo_df.to_dict('records')

    return resumo_viagens, dashboard_equipas, error_message

# --- ALTERAÇÃO: Endpoint principal atualizado ---
@app.get("/", response_class=HTMLResponse)
async def ler_relatorio(
    request: Request, 
    view_mode: str = "equipas_fixas",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    search_query: Optional[str] = None
):
    
    # --- Lógica de Data Padrão ---
    hoje = datetime.date.today()
    if not data_inicio:
        # Se não houver data de início, assume o primeiro dia do mês atual
        data_inicio = hoje.replace(day=1).isoformat()
    if not data_fim:
        # Se não houver data de fim, assume o dia de hoje
        data_fim = hoje.isoformat()
    
    # Passa a string de pesquisa (mesmo que vazia)
    search_str = search_query or ""

    # Chama a função bloqueante com os novos parâmetros
    resumo_viagens, dashboard_equipas, error_message = await run_in_threadpool(
        processar_dados_sincrono,
        data_inicio,
        data_fim,
        search_str,
        view_mode
    )
    
    # --- Renderização do Template com as novas variáveis ---
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "resumo_viagens": resumo_viagens,
        "dashboard_equipas": dashboard_equipas,
        "view_mode": view_mode,
        "data_inicio_selecionada": data_inicio, # Envia para o HTML
        "data_fim_selecionada": data_fim,       # Envia para o HTML
        "search_query": search_str,             # Envia para o HTML
        "error_message": error_message
    })