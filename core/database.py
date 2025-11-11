import os
import pandas as pd
from supabase import Client
from typing import Optional, Tuple
from .analysis import limpar_texto # Importa da mesma pasta 'core'

NOME_DA_TABELA = "Distribuição"
NOME_COLUNA_DATA = "DATA"

# --- ALTERAÇÃO: Removido o "async" ---
def get_dados_apurados(
    supabase: Client, 
    data_inicio_str: str, 
    data_fim_str: str, 
    search_str: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Busca dados do Supabase, limpa e filtra.
    Retorna o DataFrame ou (None, error_message).
    """
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
            # --- ALTERAÇÃO: Removido o "await" ---
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
        return None, "Erro ao conectar ao banco de dados." # Este é o erro que você viu

    # Limpeza de Texto
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(limpar_texto)
    
    if 'COD' in df.columns:
        df['COD'] = pd.to_numeric(df['COD'], errors='coerce')
        df.dropna(subset=['COD'], inplace=True)
        df['COD'] = df['COD'].astype(int)
    else:
         return None, "A coluna 'COD' principal não foi encontrada."

    # Filtro de Pesquisa
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