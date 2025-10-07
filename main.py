import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from supabase import create_client, Client

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv() # Carrega as variáveis do arquivo .env

app = FastAPI()
templates = Jinja2Templates(directory="templates")

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# NOME DA SUA TABELA NO SUPABASE - MUDE AQUI!
NOME_DA_TABELA = "Distribuição" # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< MUDE AQUI

# --- A LÓGICA DE ANÁLISE (A "RECEITA" QUE CRIAMOS) ---
def gerar_relatorio_equipe(df_viagens: pd.DataFrame, motorista_cod_alvo: int):
    # Parte 1: Mapeamento Global
    ajudantes_dfs = []
    for i in [1, 2, 3]:
        aj_col = f'AJUDANTE {i}'
        cod_col = f'CODJ_{i}' if i != 3 else 'CODJ_3' # Corrigido para CODJ_3
        
        # Verificar se as colunas existem antes de usar
        if aj_col in df_viagens.columns and cod_col in df_viagens.columns:
            temp_df = df_viagens[['MOTORISTA', 'COD', aj_col, cod_col]].copy()
            temp_df.rename(columns={aj_col: 'AJUDANTE_NOME', cod_col: 'AJUDANTE_COD', 'COD': 'MOTORISTA_COD'}, inplace=True)
            temp_df['POSICAO'] = f'{i}° AJUDANTE'
            ajudantes_dfs.append(temp_df)

    df_melted = pd.concat(ajudantes_dfs).dropna(subset=['AJUDANTE_COD'])
    if df_melted.empty:
        return None
        
    df_melted['AJUDANTE_COD'] = pd.to_numeric(df_melted['AJUDANTE_COD'], errors='coerce').dropna().astype(int)

    motorista_fixo_map = df_melted.groupby('AJUDANTE_COD')['MOTORISTA_COD'].apply(lambda x: x.mode()[0] if not x.mode().empty else None).to_dict()
    posicao_fixa_map = df_melted.groupby(['AJUDANTE_COD', 'MOTORISTA_COD'])['POSICAO'].apply(lambda x: x.mode()[0] if not x.mode().empty else None).to_dict()
    
    # Mapa de códigos para nomes
    cod_para_nome_map = {}
    for _, row in df_viagens[['COD', 'MOTORISTA']].dropna().drop_duplicates().iterrows(): cod_para_nome_map[row['COD']] = row['MOTORISTA']
    for _, row in df_melted[['AJUDANTE_COD', 'AJUDANTE_NOME']].dropna().drop_duplicates().iterrows(): cod_para_nome_map[row['AJUDANTE_COD']] = row['AJUDANTE_NOME']


    # Parte 2: Análise Específica
    viagens_motorista = df_viagens[df_viagens['COD'] == motorista_cod_alvo]
    total_viagens_motorista = len(viagens_motorista)
    
    viagens_melted_motorista = df_melted[df_melted['MOTORISTA_COD'] == motorista_cod_alvo]
    contagem_viagens_ajudantes = viagens_melted_motorista['AJUDANTE_COD'].value_counts().to_dict()

    equipe_fixa, visitantes = [], []

    for ajudante_cod, total_viagens in contagem_viagens_ajudantes.items():
        if motorista_fixo_map.get(ajudante_cod) == motorista_cod_alvo:
            posicao = posicao_fixa_map.get((ajudante_cod, motorista_cod_alvo))
            equipe_fixa.append({'posicao': posicao, 'nome_ajudante': cod_para_nome_map.get(ajudante_cod), 'viagens': total_viagens})
        else:
            motorista_principal_cod = motorista_fixo_map.get(ajudante_cod)
            visitantes.append({
                'nome_visitante': cod_para_nome_map.get(ajudante_cod), 
                'viagens': total_viagens, 
                'nome_motorista_principal': cod_para_nome_map.get(motorista_principal_cod)
            })
    
    return {
        "cod_motorista": motorista_cod_alvo,
        "nome_motorista": cod_para_nome_map.get(motorista_cod_alvo),
        "total_viagens": total_viagens_motorista,
        "equipe_fixa": sorted(equipe_fixa, key=lambda x: x['posicao'] or ''),
        "visitantes": visitantes,
    }


# --- O ENDPOINT DO SITE (A ROTA PRINCIPAL) ---
@app.get("/", response_class=HTMLResponse)
async def ler_relatorio(request: Request, motorista_cod: int = None):
    # Puxa TODOS os dados da tabela de viagens do Supabase
    response = supabase.table(NOME_DA_TABELA).select("COD, MOTORISTA, AJUDANTE 1, CODJ_ 1, AJUDANTE 2, CODJ_2, AJUDANTE 3, CODJ_3").execute()
    dados = response.data
    df = pd.DataFrame(dados)

    # Prepara a lista de motoristas para o menu dropdown
    motoristas_unicos = df[['COD', 'MOTORISTA']].dropna().drop_duplicates().sort_values('MOTORISTA').to_dict('records')
    
    relatorio_final = None
    if motorista_cod:
        # Se um motorista foi selecionado, gera o relatório
        relatorio_final = gerar_relatorio_equipe(df, motorista_cod)

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "motoristas": motoristas_unicos,
        "motorista_selecionado_cod": motorista_cod,
        "relatorio": relatorio_final
    })