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

# --- ALTERAÇÃO: Metas agora vêm do Supabase ---
def _get_metas_sincrono(supabase: Client) -> Dict[str, Any]:
    """
    Busca as metas reais da tabela Variavel.Metas no Supabase.
    """
    try:
        # 1. Busca os dados
        response_motorista = supabase.table("Metas", schema="Variavel").select("*").eq("tipo_colaborador", "MOTORISTA").execute()
        response_ajudante = supabase.table("Metas", schema="Variavel").select("*").eq("tipo_colaborador", "AJUDANTE").execute()

        if not response_motorista.data or not response_ajudante.data:
            # Se não encontrar dados, retorna um padrão seguro
            raise Exception("Metas não encontradas no Supabase")

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
                "refugo_premio": float(m_data["refugo_premio"])
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
                "refugo_premio": float(a_data["refugo_premio"])
            }
        }
        return metas

    except Exception as e:
        print(f"Erro ao buscar metas: {e}")
        # Retorna um dicionário vazio ou padrão em caso de erro
        return {"motorista": {}, "ajudante": {}}


@router.get("/metas", response_class=HTMLResponse)
async def ler_relatorio_metas(
    request: Request,
    supabase: Client = Depends(get_supabase) # Injeta o Supabase
):
    
    # --- ALTERAÇÃO: Busca metas reais no threadpool ---
    metas = await run_in_threadpool(_get_metas_sincrono, supabase)

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "metas",
        "metas": metas,
        # Variáveis vazias para o template não falhar
        "data_inicio_selecionada": datetime.date.today().isoformat(), # Evita erro no template
        "data_fim_selecionada": datetime.date.today().isoformat(),
        "view_mode": "equipas_fixas",
        "incentivo_tab": "motoristas",
        "search_query": "",
        "error_message": None,
        "resumo_viagens": [],
        "dashboard_equipas": [],
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
    })

# --- ALTERAÇÃO: Rota POST agora salva no Supabase ---
@router.post("/metas")
async def salvar_metas(
    request: Request,
    supabase: Client = Depends(get_supabase), # Injeta o Supabase
    # --- Metas Motorista ---
    motorista_dev_pdv_meta_perc: float = Form(...),
    motorista_dev_pdv_premio: float = Form(...),
    motorista_rating_meta_perc: float = Form(...),
    motorista_rating_premio: float = Form(...),
    motorista_refugo_meta_perc: float = Form(...),
    motorista_refugo_premio: float = Form(...),
    # --- Metas Ajudante ---
    ajudante_dev_pdv_meta_perc: float = Form(...),
    ajudante_dev_pdv_premio: float = Form(...),
    ajudante_rating_meta_perc: float = Form(...),
    ajudante_rating_premio: float = Form(...),
    ajudante_refugo_meta_perc: float = Form(...),
    ajudante_refugo_premio: float = Form(...)
):
    
    try:
        # 1. Monta os dados para o UPDATE
        dados_motorista = {
            "dev_pdv_meta_perc": motorista_dev_pdv_meta_perc,
            "dev_pdv_premio": motorista_dev_pdv_premio,
            "rating_meta_perc": motorista_rating_meta_perc,
            "rating_premio": motorista_rating_premio,
            "refugo_meta_perc": motorista_refugo_meta_perc,
            "refugo_premio": motorista_refugo_premio
        }
        
        dados_ajudante = {
            "dev_pdv_meta_perc": ajudante_dev_pdv_meta_perc,
            "dev_pdv_premio": ajudante_dev_pdv_premio,
            "rating_meta_perc": ajudante_rating_meta_perc,
            "rating_premio": ajudante_rating_premio,
            "refugo_meta_perc": ajudante_refugo_meta_perc,
            "refugo_premio": ajudante_refugo_premio
        }

        # 2. Executa o UPDATE no Supabase (em threadpool)
        await run_in_threadpool(
            supabase.table("Metas", schema="Variavel")
            .update(dados_motorista)
            .eq("tipo_colaborador", "MOTORISTA")
            .execute
        )
        
        await run_in_threadpool(
            supabase.table("Metas", schema="Variavel")
            .update(dados_ajudante)
            .eq("tipo_colaborador", "AJUDANTE")
            .execute
        )
        
        print("--- METAS SALVAS NO SUPABASE COM SUCESSO ---")

    except Exception as e:
        print(f"Erro ao salvar metas: {e}")
        # (Aqui poderíamos adicionar uma mensagem de erro para o utilizador)
    
    # Redireciona de volta para a página de metas
    return RedirectResponse(url="/metas", status_code=303)