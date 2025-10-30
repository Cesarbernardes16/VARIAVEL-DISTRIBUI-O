import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from supabase import create_client, Client
import datetime
import calendar
from typing import Optional
import unicodedata  # <--- ADICIONADO para limpeza de texto

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv()
app = FastAPI()
templates = Jinja2Templates(directory="templates")
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)
NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

# --- FUNÇÃO DE LIMPEZA DE TEXTO (CORREÇÃO DE ENCODING E MAIÚSCULAS) ---
def limpar_texto(text):
    """
    Converte para maiúsculas e remove todos os acentos e caracteres 
    inválidos (como '') de uma string.
    """
    if not isinstance(text, str):
        return text
    
    # 1. Converte para maiúsculas (ex: "João" -> "JOÃO")
    text_upper = text.upper()
    
    # 2. Normaliza para 'NFKD' para separar letras de acentos
    #    (ex: "JOÃO" -> "JOA~O")
    nfkd_form = unicodedata.normalize('NFKD', text_upper)
    
    # 3. Codifica para ASCII 'ignorando' o que não for ASCII (remove acentos e '')
    #    (ex: "JOA~O" -> b"JOAO")
    ascii_bytes = nfkd_form.encode('ASCII', 'ignore')
    
    # 4. Decodifica de volta para uma string limpa
    #    (ex: b"JOAO" -> "JOAO")
    return ascii_bytes.decode('utf-8')
# --- FIM DA FUNÇÃO ---


# --- LÓGICA DE ANÁLISE: DASHBOARD DE EQUIPAS FIXAS (VERSÃO FINAL) ---
def gerar_dashboard_equipas_fixas(df: pd.DataFrame):
    
    # --- CONSTANTES DE REGRA DE NEGÓCIO (MELHORIA) ---
    RATIO_SIGNIFICANCIA_FIXO = 0.40  # 40%
    MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA = 10
    MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA = 15
    LIMITE_VISITANTE_ESTRITO = 2  # (mostrar > 2, ou seja, 3 ou mais)
    LIMITE_VISITANTE_PADRAO = 1   # (mostrar > 1, ou seja, 2 ou mais)
    # -------------------------------------
    
    ajudantes_dfs = []
    for i in [1, 2, 3]:
        aj_col, cod_col = f'AJUDANTE_{i}', f'CODJ_{i}'
        if aj_col in df.columns and cod_col in df.columns:
            temp_df = df[['COD', aj_col, cod_col]].copy()
            temp_df.rename(columns={'COD': 'MOTORISTA_COD', aj_col: 'AJUDANTE_NOME', cod_col: 'AJUDANTE_COD'}, inplace=True)
            temp_df['POSICAO'] = f'AJUDANTE {i}'
            ajudantes_dfs.append(temp_df)
    if not ajudantes_dfs: return []

    df_global_melted = pd.concat(ajudantes_dfs)
    df_global_melted.dropna(subset=['AJUDANTE_NOME'], inplace=True)
    df_global_melted = df_global_melted[df_global_melted['AJUDANTE_NOME'].str.strip() != '']
    df_global_melted['AJUDANTE_COD'] = pd.to_numeric(df_global_melted['AJUDANTE_COD'], errors='coerce')
    df_global_melted.dropna(subset=['AJUDANTE_COD'], inplace=True)
    df_global_melted['AJUDANTE_COD'] = df_global_melted['AJUDANTE_COD'].astype(int)

    # motorista_fixo_map: Encontra o motorista MAIS frequente para cada ajudante
    motorista_fixo_map = df_global_melted.groupby('AJUDANTE_COD')['MOTORISTA_COD'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else None).to_dict()
    posicao_fixa_map = df_global_melted.groupby('AJUDANTE_COD')['POSICAO'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else 'AJUDANTE 1').to_dict()
    
    # --- INÍCIO DA CORREÇÃO: Agrupar por CÓDIGO ---
    # 1. Cria um mapa para encontrar o NOME mais frequente (correto) para cada CÓDIGO de ajudante
    nome_ajudante_map = df_global_melted.groupby('AJUDANTE_COD')['AJUDANTE_NOME'].apply(lambda x: x.mode().iloc[0] if not x.mode().empty else '').to_dict()

    # 2. Agrupa as viagens SOMENTE pelos códigos (MOTORISTA_COD e AJUDANTE_COD)
    contagem_viagens_ajudantes = df_global_melted.groupby(['MOTORISTA_COD', 'AJUDANTE_COD']).size().reset_index(name='VIAGENS')
    
    # 3. Adiciona o nome "correto" (o mais frequente) de volta ao dataframe para exibição
    contagem_viagens_ajudantes['AJUDANTE_NOME'] = contagem_viagens_ajudantes['AJUDANTE_COD'].map(nome_ajudante_map)
    # --- FIM DA CORREÇÃO ---
    
    contagem_viagens_motorista = df['COD'].value_counts().to_dict()
    
    dashboard_data = []
    colunas_motorista_base = ['COD', 'MOTORISTA']
    if 'MOTORISTA_2' in df.columns: colunas_motorista_base.append('MOTORISTA_2')
    if 'COD_2' in df.columns: colunas_motorista_base.append('COD_2')
    motoristas_no_periodo = df[colunas_motorista_base].drop_duplicates(subset=['COD'])
    
    for _, motorista_row in motoristas_no_periodo.iterrows():
        cod_motorista = int(motorista_row['COD'])
        total_viagens = contagem_viagens_motorista.get(cod_motorista, 0)
        
        # Verifica se o motorista tem nome; se não, usa o código
        nome_motorista = motorista_row.get('MOTORISTA')
        if nome_motorista is None or str(nome_motorista).strip() == '':
            nome_motorista_formatado = f"COD: {cod_motorista} ({total_viagens})"
        else:
            nome_motorista_formatado = f"{nome_motorista} ({total_viagens})"
        
        info_linha = {
            'MOTORISTA': nome_motorista_formatado, 'COD': cod_motorista,
            'MOTORISTA_2': motorista_row.get('MOTORISTA_2'), 'COD_2': motorista_row.get('COD_2'),
            'AJUDANTE_1': '', 'CODJ_1': '', 'AJUDANTE_2': '', 'CODJ_2': '', 'AJUDANTE_3': '', 'CODJ_3': '',
            'VISITANTES': []
        }
        
        viagens_com_motorista = contagem_viagens_ajudantes[contagem_viagens_ajudantes['MOTORISTA_COD'] == cod_motorista]
        
        # --- INÍCIO DA REGRA CONDICIONAL DE VISITANTES ---
        
        # 1. Separar viagens entre fixos e visitantes primeiro
        viagens_fixas = []
        viagens_visitantes = []
        for _, viagem in viagens_com_motorista.iterrows():
            viagem_data = {
                'cod_ajudante': int(viagem['AJUDANTE_COD']),
                'nome_ajudante': viagem['AJUDANTE_NOME'],
                'num_viagens': viagem['VIAGENS']
            }

            # --- INÍCIO DA NOVA LÓGICA DE CLASSIFICAÇÃO (REGRA DE SIGNIFICÂNCIA) ---
            
            # Condição 1: O ajudante é "principalmente" fixo deste motorista? (Regra antiga)
            is_primary_fixed = motorista_fixo_map.get(viagem_data['cod_ajudante']) == cod_motorista
    
            # Condição 2: O ajudante viajou uma % significativa das viagens TOTAIS do motorista? (Nova Regra)
            # Define "significativo" como > 40% (RATIO_SIGNIFICANCIA_FIXO) das viagens do motorista.
            significance_ratio = (viagem_data['num_viagens'] / total_viagens) if total_viagens > 0 else 0
            is_significant = significance_ratio > RATIO_SIGNIFICANCIA_FIXO
    
            # O ajudante é "Fixo" para este motorista se QUALQUER UMA das condições for verdadeira
            if is_primary_fixed or is_significant:
                viagem_data['posicao_fixa'] = posicao_fixa_map.get(viagem_data['cod_ajudante'], 'AJUDANTE 1')
                viagens_fixas.append(viagem_data)
            else:
                viagens_visitantes.append(viagem_data)
            # --- FIM DA NOVA LÓGICA DE CLASSIFICAÇÃO ---

        # 2. Processar os fixos e verificar a condição (> 10 viagens)
        tem_fixo_acima_de_10 = False
        for fixo in viagens_fixas:
            if fixo['num_viagens'] > MIN_VIAGENS_PARA_ATIVAR_REGRA_ESTRITA:
                tem_fixo_acima_de_10 = True
            
            posicao_str = fixo['posicao_fixa'].replace(' ', '_')
            cod_posicao_str = f"CODJ_{posicao_str.split('_')[1]}"
            info_linha[posicao_str] = f"{fixo['nome_ajudante'].strip()} ({fixo['num_viagens']})"
            info_linha[cod_posicao_str] = fixo['cod_ajudante']

        # 3. Definir o limite de viagens para visitantes com base nas condições
        condicao_motorista = total_viagens > MIN_VIAGENS_MOTORISTA_REGRA_ESTRITA
        condicao_ajudante_fixo = tem_fixo_acima_de_10
        
        # Regra Padrão: mostrar > 1 (ou seja, 2 ou mais)
        limite_minimo_visitante = LIMITE_VISITANTE_PADRAO 

        if condicao_motorista and condicao_ajudante_fixo:
            # Regra Estrita: mostrar > 2 (ou seja, 3 ou mais)
            limite_minimo_visitante = LIMITE_VISITANTE_ESTRITO

        # 4. Processar os visitantes com base no limite definido
        for visitante in viagens_visitantes:
            if visitante['num_viagens'] > limite_minimo_visitante:
                info_linha['VISITANTES'].append(f"{visitante['nome_ajudante'].strip()} ({visitante['num_viagens']}x)")
        
        # --- FIM DA REGRA CONDICIONAL DE VISITANTES ---
        
        dashboard_data.append(info_linha)
    
    # Limpa os valores 'None' antes de retornar os dados
    for linha in dashboard_data:
        for key, value in linha.items():
            if value is None:
                linha[key] = ''
        
    return sorted(dashboard_data, key=lambda x: x.get('MOTORISTA') or '')

# --- O ENDPOINT DO SITE (A ROTA PRINCIPAL) ---
@app.get("/", response_class=HTMLResponse)
async def ler_relatorio(request: Request, mes: Optional[int] = None, ano: Optional[int] = None, view_mode: str = "equipas_fixas"):
    
    hoje = datetime.date.today()
    mes_selecionado = mes if mes else hoje.month
    ano_selecionado = ano if ano else hoje.year

    primeiro_dia_str = f"{ano_selecionado}-{mes_selecionado:02d}-01"
    ultimo_dia_num = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
    ultimo_dia_str = f"{ano_selecionado}-{mes_selecionado:02d}-{ultimo_dia_num}"

    # --- MELHORIA: Tratamento de Erros e Paginação ---
    df = pd.DataFrame()
    resumo_viagens, dashboard_equipas = [], None
    error_message = None
    
    try:
        # MELHORIA: Implementa paginação para buscar TODOS os dados (remove .limit(5000))
        dados_completos = []
        page_size = 1000  # Buscar de 1000 em 1000
        page = 0
        while True:
            query = (
                supabase.table(NOME_DA_TABELA)
                .select("*")
                .gte(NOME_COLUNA_DATA, primeiro_dia_str)
                .lte(NOME_COLUNA_DATA, ultimo_dia_str)
                .range(page * page_size, (page + 1) * page_size - 1)
            )
            response = query.execute()
            
            if not response.data:
                break # Sai do loop se não houver mais dados
                
            dados_completos.extend(response.data)
            page += 1
            
            # Otimização: para se a última página não estava cheia
            if len(response.data) < page_size:
                break
        
        if not dados_completos:
            error_message = "Nenhum dado encontrado para a seleção atual."
        else:
            df = pd.DataFrame(dados_completos)

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase: {e}")
        error_message = "Erro ao conectar ao banco de dados. Tente novamente mais tarde."
        # Mesmo com erro, continuamos para renderizar a página com a mensagem
    # --- FIM DA MELHORIA ---

    if not df.empty:
        
        # --- CORREÇÃO DE ENCODING E MAIÚSCULAS ---
        # Aplica a função de limpeza em todas as colunas de texto (tipo 'object')
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].apply(limpar_texto)
        # --- FIM DA CORREÇÃO ---
        
        # Garante que a coluna COD existe antes de tentar processá-la
        if 'COD' in df.columns:
            df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
            df.dropna(subset=['COD'], inplace=True)
            df['COD'] = df['COD'].astype(int)
        else:
             # Se não houver 'COD', retorna vazio para evitar erros
             df = pd.DataFrame() 
             if not error_message: # Só define msg de erro se já não houver uma
                 error_message = "A coluna 'COD' principal não foi encontrada nos dados."

    if not df.empty:
        if view_mode == 'equipas_fixas':
            dashboard_equipas = gerar_dashboard_equipas_fixas(df)
        else: 
            colunas_resumo = ['MAPA', 'MOTORISTA', 'COD', 'MOTORISTA_2', 'COD_2', 'AJUDANTE_1', 'CODJ_1', 'AJUDANTE_2', 'CODJ_2', 'AJUDANTE_3', 'CODJ_3']
            colunas_existentes = [col for col in colunas_resumo if col in df.columns]
            resumo_df = df[colunas_existentes].sort_values(by='MOTORISTA' if 'MOTORISTA' in colunas_existentes else colunas_existentes[0])
            resumo_df.fillna('', inplace=True) # Substitui NaN/None por string vazia
            resumo_viagens = resumo_df.to_dict('records')

    meses_do_ano = [{"num": i, "nome": calendar.month_name[i]} for i in range(1, 13)]
    
    # Adiciona localização para Português do Brasil para nomes dos meses
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        meses_do_ano = [{"num": i, "nome": calendar.month_name[i].capitalize()} for i in range(1, 13)]
    except Exception as e:
        # Se der erro (ex: locale não instalado), mantém o padrão em inglês
        pass 

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "resumo_viagens": resumo_viagens,
        "dashboard_equipas": dashboard_equipas,
        "view_mode": view_mode,
        "meses": meses_do_ano,
        "mes_selecionado": mes_selecionado,
        "ano_selecionado": ano_selecionado,
        "error_message": error_message  # <-- ADICIONADO para mostrar erros
    })