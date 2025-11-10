import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
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

# --- FUNÇÃO DE LIMPEZA DE TEXTO ---
def limpar_texto(text):
    if not isinstance(text, str):
        return text
    text_upper = text.upper()
    nfkd_form = unicodedata.normalize('NFKD', text_upper)
    ascii_bytes = nfkd_form.encode('ASCII', 'ignore')
    return ascii_bytes.decode('utf-8')

# --- LÓGICA DE ANÁLISE "XADREZ" (Funções Principais) ---
# (Estas funções permanecem inalteradas)

def _preparar_dataframe_ajudantes(df: pd.DataFrame) -> pd.DataFrame:
    ajudantes_dfs = []
    colunas_ajudante = sorted(df.filter(regex=r'^AJUDANTE_\d+$').columns)
    for aj_col in colunas_ajudante:
        num = aj_col.split('_')[-1]
        cod_col = f'CODJ_{num}'
        if cod_col in df.columns:
            temp_df = df[['COD', 'MOTORISTA', aj_col, cod_col]].copy()
            temp_df.rename(columns={
                'COD': 'MOTORISTA_COD', 
                'MOTORISTA': 'MOTORISTA_NOME',
                aj_col: 'AJUDANTE_NOME', 
                cod_col: 'AJUDANTE_COD'
            }, inplace=True)
            temp_df['POSICAO'] = f'AJUDANTE {num}'
            ajudantes_dfs.append(temp_df)
    if not ajudantes_dfs:
        return pd.DataFrame(columns=['MOTORISTA_COD', 'MOTORISTA_NOME', 'AJUDANTE_NOME', 'AJUDANTE_COD', 'POSICAO'])
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
    motorista_nome_map = df_original.drop_duplicates(subset=['COD']).set_index('COD')['MOTORISTA'].to_dict()
    return {
        "motorista_fixo_map": motorista_fixo_map,
        "posicao_fixa_map": posicao_fixa_map,
        "nome_ajudante_map": nome_ajudante_map,
        "contagem_viagens_motorista": contagem_viagens_motorista,
        "motorista_nome_map": motorista_nome_map
    }

def _classificar_e_atribuir_viagens(info_linha, viagens_com_motorista, mapas, total_viagens, regras):
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

def gerar_dashboard_e_mapas(df: pd.DataFrame) -> dict:
    regras = {
        "RATIO_SIGNIFICANCIA_FIXO": 0.40,
        "MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA": 10,
        "MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA": 15,
        "LIMITE_VISITANTE_ESTRITO": 2,
        "LIMITE_VISITANTE_PADRAO": 1,
    }
    df_melted = _preparar_dataframe_ajudantes(df)
    if df_melted.empty:
        return {"dashboard_data": [], "mapas": {}, "df_melted": df_melted}
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
        info_linha = {'MOTORISTA': nome_formatado, 'COD': cod_motorista, 'MOTORISTA_2': motorista_row.get('MOTORISTA_2'), 'COD_2': motorista_row.get('COD_2'), 'VISITANTES': []}
        max_pos = df_melted['POSICAO'].nunique() if not df_melted.empty else 3
        for i in range(1, max_pos + 1):
            info_linha[f'AJUDANTE_{i}'] = ''
            info_linha[f'CODJ_{i}'] = ''
        viagens_com_motorista = contagem_viagens_ajudantes[contagem_viagens_ajudantes['MOTORISTA_COD'] == cod_motorista]
        _classificar_e_atribuir_viagens(info_linha, viagens_com_motorista, mapas, total_viagens, regras)
        dashboard_data.append(info_linha)
    for linha in dashboard_data:
        for key, value in linha.items():
            if value is None:
                linha[key] = ''
    dashboard_final = sorted(dashboard_data, key=lambda x: x.get('MOTORISTA') or '')
    return {"dashboard_data": dashboard_final, "mapas": mapas, "df_melted": df_melted}

# --- FUNÇÃO DE BUSCA DE DADOS CENTRALIZADA ---
def _get_dados_apurados(data_inicio_str: str, data_fim_str: str, search_str: str):
    df = pd.DataFrame()
    error_message = None
    try:
        dados_completos = []
        page_size = 1000
        page = 0
        while True:
            query = (
                supabase.table(NOME_DA_TABELA)
                .select("*")
                .gte(NOME_COLUNA_DATA, data_inicio_str)
                .lte(NOME_COLUNA_DATA, data_fim_str)
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute()
            if not response.data: break
            dados_completos.extend(response.data)
            page += 1
            if len(response.data) < page_size: break
        if not dados_completos:
            return None, "Nenhum dado encontrado para o período selecionado."
        df = pd.DataFrame(dados_completos)
    except Exception as e:
        print(f"Erro ao buscar dados do Supabase: {e}")
        return None, "Erro ao conectar ao banco de dados."
    
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(limpar_texto)
    if 'COD' in df.columns:
        df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
        df.dropna(subset=['COD'], inplace=True)
        df['COD'] = df['COD'].astype(int)
    else:
         return None, "A coluna 'COD' principal não foi encontrada."
    if search_str:
        search_clean = limpar_texto(search_str)
        colunas_busca = ['MOTORISTA', 'MOTORISTA_2', 'AJUDANTE_1', 'AJUDANTE_2', 'AJUDANTE_3']
        colunas_existentes_busca = [col for col in colunas_busca if col in df.columns]
        mask = pd.Series(False, index=df.index)
        for col in colunas_existentes_busca:
            mask = mask | df[col].str.contains(search_clean, na=False)
        df = df[mask]
        if df.empty:
            return None, f"Nenhum dado encontrado para o termo de busca: '{search_str}'"
    return df, None

# --- FUNÇÃO DE PROCESSAMENTO (Aba Xadrez) ---
def processar_xadrez_sincrono(data_inicio_str, data_fim_str, search_str, view_mode):
    resumo_viagens, dashboard_equipas = [], None
    df, error_message = _get_dados_apurados(data_inicio_str, data_fim_str, search_str)
    if error_message:
        return resumo_viagens, dashboard_equipas, error_message
    if view_mode == 'equipas_fixas':
        resultado_xadrez = gerar_dashboard_e_mapas(df)
        dashboard_equipas = resultado_xadrez["dashboard_data"]
    else: 
        colunas_resumo = ['MAPA', 'MOTORISTA', 'COD', 'MOTORISTA_2', 'COD_2', 'AJUDANTE_1', 'CODJ_1', 'AJUDANTE_2', 'CODJ_2', 'AJUDANTE_3', 'CODJ_3']
        colunas_existentes = [col for col in colunas_resumo if col in df.columns]
        resumo_df = df[colunas_existentes].sort_values(by='MOTORISTA' if 'MOTORISTA' in colunas_existentes else colunas_existentes[0])
        resumo_df.fillna('', inplace=True)
        resumo_viagens = resumo_df.to_dict('records')
    return resumo_viagens, dashboard_equipas, error_message

# --- FUNÇÃO DE PROCESSAMENTO (Aba Incentivo) - MODIFICADA ---
def processar_incentivos_sincrono(data_inicio_str: str, data_fim_str: str):
    
    incentivo_motoristas = []
    incentivo_ajudantes = []
    error_message = None
    metas = {}

    try:
        # --- ETAPA 1: Gerar dados FALSOS (MOCK) para Motoristas ---
        metas = {
            "dev_pdv_meta": "2,64%", "dev_pdv_premio": 160.00,
            "rating_meta": "35,07%", "rating_premio": 100.00,
            "refugo_meta": "1.0%", "refugo_premio": 100.00
        }
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
            linha["dev_pdv_premio_val"] = metas["dev_pdv_premio"] if (dev_atingido is not None and dev_atingido <= 2.64) else 0.0
            
            rating_atingido = linha.get("rating")
            linha["rating_val"] = f"{rating_atingido:.2f}%" if rating_atingido is not None else "N/A"
            linha["rating_premio_val"] = metas["rating_premio"] if (rating_atingido is not None and rating_atingido >= 35.07) else 0.0

            refugo_atingido = linha.get("refugo")
            linha["refugo_val"] = f"{refugo_atingido:.2f}%" if refugo_atingido is not None else "N/A"
            linha["refugo_premio_val"] = metas["refugo_premio"] if (refugo_atingido is not None and refugo_atingido <= 1.0) else 0.0

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

        # --- ETAPA 2: Buscar dados reais e ligar Ajudantes ---
        df, error_message = _get_dados_apurados(data_inicio_str, data_fim_str, search_str="")
        
        if error_message and not df:
             return incentivo_motoristas, [], error_message, metas

        resultado_xadrez = gerar_dashboard_e_mapas(df)
        mapas = resultado_xadrez["mapas"]
        df_melted = resultado_xadrez["df_melted"]
        
        motorista_fixo_map = mapas.get("motorista_fixo_map", {})
        
        ajudantes_unicos = df_melted.drop_duplicates(subset=['AJUDANTE_COD'])
        
        for _, ajudante in ajudantes_unicos.iterrows():
            cod_ajudante = ajudante['AJUDANTE_COD']
            nome_ajudante = ajudante['AJUDANTE_NOME']
            cod_motorista_fixo = motorista_fixo_map.get(cod_ajudante)
            
            if cod_motorista_fixo:
                premio_info_herdado = premio_motorista_map.get(cod_motorista_fixo, default_premio_info)
            else:
                premio_info_herdado = default_premio_info.copy()
            
            # --- ALTERAÇÃO ---
            # Dicionário do ajudante não precisa mais do motorista_fixo
            ajudante_data = {
                "cod": cod_ajudante,
                "nome": nome_ajudante,
            }
            # --- FIM DA ALTERAÇÃO ---
            
            ajudante_data.update(premio_info_herdado)
            incentivo_ajudantes.append(ajudante_data)
            
        incentivo_ajudantes = sorted(incentivo_ajudantes, key=lambda x: x['nome'])

    except Exception as e:
        print(f"Erro ao gerar dados de incentivo: {e}")
        error_message = "Erro ao processar dados de incentivo."
        
    return incentivo_motoristas, incentivo_ajudantes, error_message, metas

# --- ROTA PRINCIPAL (Inalterada) ---
@app.get("/", response_class=HTMLResponse)
async def ler_relatorio(
    request: Request, 
    main_tab: str = "xadrez",
    view_mode: str = "equipas_fixas",
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    search_query: Optional[str] = None
):
    
    hoje = datetime.date.today()
    if not data_inicio:
        data_inicio = hoje.replace(day=1).isoformat()
    if not data_fim:
        data_fim = hoje.isoformat()
    search_str = search_query or ""

    resumo_viagens, dashboard_equipas = [], None
    incentivo_motoristas, incentivo_ajudantes = [], []
    metas = {}
    error_message = None

    if main_tab == "xadrez":
        resumo_viagens, dashboard_equipas, error_message = await run_in_threadpool(
            processar_xadrez_sincrono,
            data_inicio,
            data_fim,
            search_str,
            view_mode
        )
    
    elif main_tab == "incentivo":
        incentivo_motoristas, incentivo_ajudantes, error_message, metas = await run_in_threadpool(
            processar_incentivos_sincrono,
            data_inicio,
            data_fim
        )
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": main_tab,
        "view_mode": view_mode,
        "data_inicio_selecionada": data_inicio,
        "data_fim_selecionada": data_fim,
        "search_query": search_str,
        "error_message": error_message,
        "resumo_viagens": resumo_viagens,
        "dashboard_equipas": dashboard_equipas,
        "incentivo_motoristas": incentivo_motoristas,
        "incentivo_ajudantes": incentivo_ajudantes,
        "metas": metas
    })

# --- ROTA DO FAVICON (Inalterada) ---
@app.get("/favicon.ico", include_in_schema=False)
async def favicon_route():
    return Response(status_code=204)