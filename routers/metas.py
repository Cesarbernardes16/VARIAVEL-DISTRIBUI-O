import datetime
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict, Any
from supabase import Client # Importar o Client
from fastapi.concurrency import run_in_threadpool # Importar o run_in_threadpool

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Função para obter o cliente Supabase do estado da request
def get_supabase(request: Request) -> Client:
    return request.state.supabase

# --- FUNÇÃO DE FALLBACK ---
def _get_default_metas() -> Dict[str, Any]:
    """
    Retorna uma estrutura de metas padrão para evitar crash no template
    caso o Supabase falhe ou esteja vazio.
    """
    default_values = {
        "dev_pdv_meta_perc": 0.0, "dev_pdv_meta": "N/A", "dev_pdv_premio": 0.0,
        "rating_meta_perc": 0.0, "rating_meta": "N/A", "rating_premio": 0.0,
        "refugo_meta_perc": 0.0, "refugo_meta": "N/A", "refugo_premio": 0.0
    }
    
    # --- NOVO: Fallback para as metas de caixa (usando nomes genéricos) ---
    default_caixas = {
        "meta_cx_dias_n1": 365, "meta_cx_valor_n1": 0.0,
        "meta_cx_dias_n2": 730, "meta_cx_valor_n2": 0.0,
        "meta_cx_dias_n3": 1825, "meta_cx_valor_n3": 0.0,
        "meta_cx_dias_n4": 9999, "meta_cx_valor_n4": 0.0, 
    }
    
    # Combina os defaults
    default_values.update(default_caixas)
    
    return {
        "motorista": default_values.copy(),
        "ajudante": default_values.copy()
    }
# --- FIM DA FUNÇÃO ---


# --- ALTERAÇÃO: Metas agora vêm do Supabase ---
def _get_metas_sincrono(supabase: Client) -> Dict[str, Any]:
    """
    Busca as metas reais da tabela Variavel.Metas no Supabase.
    """
    try:
        # 1. Busca os dados
        response_motorista = supabase.table("Metas").select("*").eq("tipo_colaborador", "MOTORISTA").execute()
        response_ajudante = supabase.table("Metas").select("*").eq("tipo_colaborador", "AJUDANTE").execute()

        if not response_motorista.data or not response_ajudante.data:
            raise Exception("Metas não encontradas no Supabase (tabela 'Metas' está vazia?)")

        m_data = response_motorista.data[0]
        a_data = response_ajudante.data[0]

        # 2. Formata os dados para o template
        metas = {
            "motorista": {
                "dev_pdv_meta_perc": float(m_data["dev_pdv_meta_perc"]),
                "dev_pdv_meta": f"{float(m_data['dev_pdv_meta_perc']):.2f}%", 
                "dev_pdv_premio": float(m_data["dev_pdv_premio"]),
                
                "rating_meta_perc": float(m_data["rating_meta_perc"]),
                "rating_meta": f"{float(m_data['rating_meta_perc']):.2f}%", 
                "rating_premio": float(m_data["rating_premio"]),
                
                "refugo_meta_perc": float(m_data["refugo_meta_perc"]),
                "refugo_meta": f"{float(m_data['refugo_meta_perc']):.1f}%", 
                "refugo_premio": float(m_data["refugo_premio"]),
                
                # --- CORRIGIDO: Lê as metas de caixa (nomes genéricos) ---
                # Vamos ler do motorista, assumindo que são iguais
                "meta_cx_dias_n1": int(m_data.get("meta_cx_dias_n1", 365)),
                "meta_cx_valor_n1": float(m_data.get("meta_cx_valor_n1", 0)),
                "meta_cx_dias_n2": int(m_data.get("meta_cx_dias_n2", 730)),
                "meta_cx_valor_n2": float(m_data.get("meta_cx_valor_n2", 0)),
                "meta_cx_dias_n3": int(m_data.get("meta_cx_dias_n3", 1825)),
                "meta_cx_valor_n3": float(m_data.get("meta_cx_valor_n3", 0)),
                "meta_cx_dias_n4": int(m_data.get("meta_cx_dias_n4", 9999)), 
                "meta_cx_valor_n4": float(m_data.get("meta_cx_valor_n4", 0)), 
            },
            "ajudante": {
                "dev_pdv_meta_perc": float(a_data["dev_pdv_meta_perc"]),
                "dev_pdv_meta": f"{float(a_data['dev_pdv_meta_perc']):.2f}%", 
                "dev_pdv_premio": float(a_data["dev_pdv_premio"]),
                
                "rating_meta_perc": float(a_data["rating_meta_perc"]),
                "rating_meta": f"{float(a_data['rating_meta_perc']):.2f}%", 
                "rating_premio": float(a_data["rating_premio"]),
                
                "refugo_meta_perc": float(a_data["refugo_meta_perc"]),
                "refugo_meta": f"{float(a_data['refugo_meta_perc']):.1f}%", 
                "refugo_premio": float(a_data["refugo_premio"]),
                
                # --- CORRIGIDO: Lê as metas de caixa (nomes genéricos) ---
                "meta_cx_dias_n1": int(a_data.get("meta_cx_dias_n1", 365)),
                "meta_cx_valor_n1": float(a_data.get("meta_cx_valor_n1", 0)),
                "meta_cx_dias_n2": int(a_data.get("meta_cx_dias_n2", 730)),
                "meta_cx_valor_n2": float(a_data.get("meta_cx_valor_n2", 0)),
                "meta_cx_dias_n3": int(a_data.get("meta_cx_dias_n3", 1825)),
                "meta_cx_valor_n3": float(a_data.get("meta_cx_valor_n3", 0)),
                "meta_cx_dias_n4": int(a_data.get("meta_cx_dias_n4", 9999)), 
                "meta_cx_valor_n4": float(a_data.get("meta_cx_valor_n4", 0)), 
            }
        }
        return metas

    except Exception as e:
        print(f"Erro ao buscar metas: {e}")
        return _get_default_metas()


@router.get("/metas", response_class=HTMLResponse)
async def ler_relatorio_metas(
    request: Request,
    supabase: Client = Depends(get_supabase) # Injeta o Supabase
):
    
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "metas",
        "metas": metas,
        "data_inicio_selecionada": datetime.date.today().isoformat(),
        "data_fim_selecionada": datetime.date.today().isoformat(),
        "view_mode": "equipas_fixas",
        "incentivo_tab": "motoristas",
        "search_query": "",
        "error_message": None,
        "resumo_viagens": [],
        "dashboard_equipas": [],
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
        "caixas_tab": "motoristas",
    })

# --- ALTERAÇÃO: Rota POST agora salva no Supabase ---
@router.post("/metas")
async def salvar_metas(
    request: Request,
    supabase: Client = Depends(get_supabase), # Injeta o Supabase
    
    # --- Metas Indicadores (Existentes) ---
    motorista_dev_pdv_meta_perc: float = Form(...),
    motorista_dev_pdv_premio: float = Form(...),
    motorista_rating_meta_perc: float = Form(...),
    motorista_rating_premio: float = Form(...),
    motorista_refugo_meta_perc: float = Form(...),
    motorista_refugo_premio: float = Form(...),
    ajudante_dev_pdv_meta_perc: float = Form(...),
    ajudante_dev_pdv_premio: float = Form(...),
    ajudante_rating_meta_perc: float = Form(...),
    ajudante_rating_premio: float = Form(...),
    ajudante_refugo_meta_perc: float = Form(...),
    ajudante_refugo_premio: float = Form(...),
    
    # --- CORRIGIDO: Metas Caixas (Valores únicos) ---
    meta_cx_dias_n1: int = Form(...),
    meta_cx_valor_n1: float = Form(...),
    meta_cx_dias_n2: int = Form(...),
    meta_cx_valor_n2: float = Form(...),
    meta_cx_dias_n3: int = Form(...),
    meta_cx_valor_n3: float = Form(...),
    meta_cx_dias_n4: int = Form(...),
    meta_cx_valor_n4: float = Form(...)
):
    
    try:
        # 1. Monta os dados para o UPDATE
        
        # --- CORRIGIDO: Define os valores de caixas uma vez ---
        dados_caixas_comuns = {
            "meta_cx_dias_n1": meta_cx_dias_n1,
            "meta_cx_valor_n1": meta_cx_valor_n1,
            "meta_cx_dias_n2": meta_cx_dias_n2,
            "meta_cx_valor_n2": meta_cx_valor_n2,
            "meta_cx_dias_n3": meta_cx_dias_n3,
            "meta_cx_valor_n3": meta_cx_valor_n3,
            "meta_cx_dias_n4": meta_cx_dias_n4,
            "meta_cx_valor_n4": meta_cx_valor_n4,
        }
        
        # 2. Prepara os dados do Motorista
        dados_motorista = {
            "dev_pdv_meta_perc": motorista_dev_pdv_meta_perc,
            "dev_pdv_premio": motorista_dev_pdv_premio,
            "rating_meta_perc": motorista_rating_meta_perc,
            "rating_premio": motorista_rating_premio,
            "refugo_meta_perc": motorista_refugo_meta_perc,
            "refugo_premio": motorista_refugo_premio,
        }
        dados_motorista.update(dados_caixas_comuns) # Adiciona os valores comuns
        
        # 3. Prepara os dados do Ajudante
        dados_ajudante = {
            "dev_pdv_meta_perc": ajudante_dev_pdv_meta_perc,
            "dev_pdv_premio": ajudante_dev_pdv_premio,
            "rating_meta_perc": ajudante_rating_meta_perc,
            "rating_premio": ajudante_rating_premio,
            "refugo_meta_perc": ajudante_refugo_meta_perc,
            "refugo_premio": ajudante_refugo_premio,
        }
        dados_ajudante.update(dados_caixas_comuns) # Adiciona os valores comuns

        # 4. Executa o UPDATE no Supabase
        await run_in_threadpool(
            supabase.table("Metas")
            .update(dados_motorista)
            .eq("tipo_colaborador", "MOTORISTA")
            .execute
        )
        
        await run_in_threadpool(
            supabase.table("Metas")
            .update(dados_ajudante)
            .eq("tipo_colaborador", "AJUDANTE")
            .execute
        )
        
        print("--- METAS (COMUNS E INDICADORES) SALVAS NO SUPABASE COM SUCESSO ---")

    except Exception as e:
        print(f"Erro ao salvar metas: {e}")
    
    # Redireciona de volta para a página de metas
    return RedirectResponse(url="/metas", status_code=303)
