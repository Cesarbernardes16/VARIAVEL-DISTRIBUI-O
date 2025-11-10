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
# --- MELHORIA DE PERFORMANCE ---
# Importa o run_in_threadpool para rodar código síncrono (pandas/supabase)
# sem bloquear o loop de eventos principal do FastAPI.
from fastapi.concurrency import run_in_threadpool
# --- FIM DA MELHORIA ---

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)
NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

# --- MELHORIA DE LOCALIZAÇÃO ---
# Define a localização para pt_BR globalmente, uma única vez.
try:
    import locale
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    MESES_DO_ANO = [{"num": i, "nome": calendar.month_name[i].capitalize()} for i in range(1, 13)]
except Exception as e:
    # Se der erro (ex: locale não instalado), mantém o padrão em inglês
    MESES_DO_ANO = [{"num": i, "nome": calendar.month_name[i]} for i in range(1, 13)]
# --- FIM DA MELHORIA ---

# --- FUNÇÃO DE LIMPEZA DE TEXTO (Inalterada, já estava ótima) ---
def limpar_texto(text):
    if not isinstance(text, str):
        return text
    text_upper = text.upper()
    nfkd_form = unicodedata.normalize('NFKD', text_upper)
    ascii_bytes = nfkd_form.encode('ASCII', 'ignore')
    return ascii_bytes.decode('utf-8')

# --- LÓGICA DE ANÁLISE: DASHBOARD DE EQUIPAS FIXAS (Refatorado para Legibilidade) ---

# --- MELHORIA ESTRUTURAL: Função Auxiliar 1 ---
def _preparar_dataframe_ajudantes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame de formato "largo" (AJUDANTE_1, CODJ_1, ...) 
    para "longo", facilitando a agregação.
    """
    ajudantes_dfs = []
    
    # MELHORIA: Torna a busca de colunas dinâmica (não fixa em 1, 2, 3)
    colunas_ajudante = sorted(df.filter(regex=r'^AJUDANTE_\d+$').columns)

    for aj_col in colunas_ajudante:
        # Extrai o número (ex: "1" de "AJUDANTE_1")
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

# --- MELHORIA ESTRUTURAL: Função Auxiliar 2 ---
def _calcular_mapas_referencia(df_melted: pd.DataFrame, df_original: pd.DataFrame) -> dict:
    """
    Calcula todos os dicionários de "lookup" necessários para a análise.
    """
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

# --- MELHORIA ESTRUTURAL: Função Auxiliar 3 ---
def _classificar_e_atribuir_viagens(
    info_linha: Dict[str, Any], 
    viagens_com_motorista: pd.DataFrame, 
    mapas: Dict[str, Any], 
    total_viagens: int,
    regras: Dict[str, Any]
):
    """
    Aplica a lógica de "Fixo" vs "Visitante" e preenche a info_linha.
    """
    viagens_fixas = []
    viagens_visitantes = []
    
    # 1. Classifica todas as viagens como fixas ou visitantes
    for _, viagem in viagens_com_motorista.iterrows():
        viagem_data = {
            'cod_ajudante': int(viagem['AJUDANTE_COD']),
            'nome_ajudante': viagem['AJUDANTE_NOME'],
            'num_viagens': viagem['VIAGENS']
        }

        # Condição 1: O ajudante é "principalmente" fixo deste motorista?
        is_primary_fixed = mapas["motorista_fixo_map"].get(viagem_data['cod_ajudante']) == info_linha['COD']

        # Condição 2: O ajudante viajou uma % significativa?
        significance_ratio = (viagem_data['num_viagens'] / total_viagens) if total_viagens > 0 else 0
        is_significant = significance_ratio > regras["RATIO_SIGNIFICANCIA_FIXO"]

        if is_primary_fixed or is_significant:
            viagem_data['posicao_fixa'] = mapas["posicao_fixa_map"].get(viagem_data['cod_ajudante'], 'AJUDANTE 1')
            viagens_fixas.append(viagem_data)
        else:
            viagens_visitantes.append(viagem_data)

    # 2. Processa os fixos e verifica a "regra estrita"
    tem_fixo_acima_de_10 = False
    for fixo in viagens_fixas:
        if fixo['num_viagens'] > regras["MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA"]:
            tem_fixo_acima_de_10 = True
        
        posicao_str = fixo['posicao_fixa'].replace(' ', '_')
        cod_posicao_str = f"CODJ_{posicao_str.split('_')[-1]}"
        info_linha[posicao_str] = f"{fixo['nome_ajudante'].strip()} ({fixo['num_viagens']})"
        info_linha[cod_posicao_str] = fixo['cod_ajudante']

    # 3. Define o limite de viagens para visitantes
    condicao_motorista = total_viagens > regras["MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA"]
    
    limite_minimo_visitante = regras["LIMITE_VISITANTE_PADRAO"]
    if condicao_motorista and tem_fixo_acima_de_10:
        limite_minimo_visitante = regras["LIMITE_VISITANTE_ESTRITO"]

    # 4. Processa os visitantes com base no limite
    for visitante in viagens_visitantes:
        if visitante['num_viagens'] > limite_minimo_visitante:
            info_linha['VISITANTES'].append(f"{visitante['nome_ajudante'].strip()} ({visitante['num_viagens']}x)")

# --- MELHORIA ESTRUTURAL: Função Principal de Análise (Orquestradora) ---
def gerar_dashboard_equipas_fixas(df: pd.DataFrame) -> List[Dict[str, Any]]:
    
    # Constantes de Regra de Negócio
    regras = {
        "RATIO_SIGNIFICANCIA_FIXO": 0.40,
        "MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA": 10,
        "MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA": 15,
        "LIMITE_VISITANTE_ESTRITO": 2,
        "LIMITE_VISITANTE_PADRAO": 1,
    }
    
    # 1. Preparar Dados (Melt)
    df_melted = _preparar_dataframe_ajudantes(df)
    if df_melted.empty:
        return []

    # 2. Calcular Mapas de Referência
    mapas = _calcular_mapas_referencia(df_melted, df)
    
    # 3. Corrigir Nomes e Contagens (Agrupar por CÓDIGO)
    contagem_viagens_ajudantes = df_melted.groupby(['MOTORISTA_COD', 'AJUDANTE_COD']).size().reset_index(name='VIAGENS')
    contagem_viagens_ajudantes['AJUDANTE_NOME'] = contagem_viagens_ajudantes['AJUDANTE_COD'].map(mapas["nome_ajudante_map"])
    
    # 4. Iterar por Motorista e Gerar Linhas do Dashboard
    dashboard_data = []
    colunas_motorista_base = ['COD', 'MOTORISTA', 'MOTORISTA_2', 'COD_2']
    colunas_existentes = [col for col in colunas_motorista_base if col in df.columns]
    motoristas_no_periodo = df[colunas_existentes].drop_duplicates(subset=['COD'])
    
    for _, motorista_row in motoristas_no_periodo.iterrows():
        cod_motorista = int(motorista_row['COD'])
        total_viagens = mapas["contagem_viagens_motorista"].get(cod_motorista, 0)
        
        nome_motorista = motorista_row.get('MOTORISTA')
        if nome_motorista is None or str(nome_motorista).strip() == '':
            nome_motorista_formatado = f"COD: {cod_motorista} ({total_viagens})"
        else:
            nome_motorista_formatado = f"{nome_motorista} ({total_viagens})"
        
        # Prepara a linha de saída
        info_linha = {
            'MOTORISTA': nome_motorista_formatado, 'COD': cod_motorista,
            'MOTORISTA_2': motorista_row.get('MOTORISTA_2'), 'COD_2': motorista_row.get('COD_2'),
            'VISITANTES': []
        }
        # Adiciona posições de ajudante dinamicamente (para AJUDANTE_4, 5...)
        max_pos = df_melted['POSICAO'].nunique()
        for i in range(1, max_pos + 1):
            info_linha[f'AJUDANTE_{i}'] = ''
            info_linha[f'CODJ_{i}'] = ''

        
        viagens_com_motorista = contagem_viagens_ajudantes[contagem_viagens_ajudantes['MOTORISTA_COD'] == cod_motorista]
        
        # 5. Classificar Viagens (Fixo vs Visitante) e preencher a linha
        _classificar_e_atribuir_viagens(
            info_linha, 
            viagens_com_motorista, 
            mapas, 
            total_viagens,
            regras
        )
        
        dashboard_data.append(info_linha)
    
    # Limpa os valores 'None'
    for linha in dashboard_data:
        for key, value in linha.items():
            if value is None:
                linha[key] = ''
        
    return sorted(dashboard_data, key=lambda x: x.get('MOTORISTA') or '')

# --- MELHORIA DE PERFORMANCE: Função Síncrona Bloqueante ---
def processar_dados_sincrono(ano: int, mes: int, view_mode: str):
    """
    Esta função contém TODO o código bloqueante (I/O de rede e processamento
    pesado de Pandas) e foi projetada para ser executada em um thread pool.
    """
    primeiro_dia_str = f"{ano}-{mes:02d}-01"
    ultimo_dia_num = calendar.monthrange(ano, mes)[1]
    ultimo_dia_str = f"{ano}-{mes:02d}-{ultimo_dia_num}"

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
                .gte(NOME_COLUNA_DATA, primeiro_dia_str)
                .lte(NOME_COLUNA_DATA, ultimo_dia_str)
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute() # <-- BLOQUEANTE
            
            if not response.data:
                break
            dados_completos.extend(response.data)
            page += 1
            if len(response.data) < page_size:
                break
        
        if not dados_completos:
            error_message = "Nenhum dado encontrado para a seleção atual."
        else:
            df = pd.DataFrame(dados_completos) # <-- PROCESSAMENTO PESADO

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase: {e}")
        error_message = "Erro ao conectar ao banco de dados. Tente novamente mais tarde."
        # Retorna a tupla com os valores padrão e a mensagem de erro
        return resumo_viagens, dashboard_equipas, error_message

    if not df.empty:
        # Limpeza e processamento (Bloqueante)
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

    if not df.empty:
        # Análise de dados (O mais bloqueante de todos)
        if view_mode == 'equipas_fixas':
            dashboard_equipas = gerar_dashboard_equipas_fixas(df) # <-- PROCESSAMENTO PESADO
        else: 
            colunas_resumo = ['MAPA', 'MOTORISTA', 'COD', 'MOTORISTA_2', 'COD_2', 'AJUDANTE_1', 'CODJ_1', 'AJUDANTE_2', 'CODJ_2', 'AJUDANTE_3', 'CODJ_3']
            colunas_existentes = [col for col in colunas_resumo if col in df.columns]
            resumo_df = df[colunas_existentes].sort_values(by='MOTORISTA' if 'MOTORISTA' in colunas_existentes else colunas_existentes[0])
            resumo_df.fillna('', inplace=True)
            resumo_viagens = resumo_df.to_dict('records')

    # Retorna os dados processados
    return resumo_viagens, dashboard_equipas, error_message

# --- O ENDPOINT DO SITE (A ROTA PRINCIPAL) ---
# --- MELHORIA DE PERFORMANCE: Endpoint agora é assíncrono de verdade ---
@app.get("/", response_class=HTMLResponse)
async def ler_relatorio(request: Request, mes: Optional[int] = None, ano: Optional[int] = None, view_mode: str = "equipas_fixas"):
    
    hoje = datetime.date.today()
    mes_selecionado = mes if mes else hoje.month
    ano_selecionado = ano if ano else hoje.year

    # Chama a função bloqueante (Supabase + Pandas) em um thread pool
    # e espera (await) pelo seu resultado sem bloquear o servidor.
    resumo_viagens, dashboard_equipas, error_message = await run_in_threadpool(
        processar_dados_sincrono,
        ano_selecionado,
        mes_selecionado,
        view_mode
    )
    
    # O restante (renderização do template) é rápido e pode
    # ser feito no loop principal.
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "resumo_viagens": resumo_viagens,
        "dashboard_equipas": dashboard_equipas,
        "view_mode": view_mode,
        "meses": MESES_DO_ANO, # Usa a constante global
        "mes_selecionado": mes_selecionado,
        "ano_selecionado": ano_selecionado,
        "error_message": error_message
    })