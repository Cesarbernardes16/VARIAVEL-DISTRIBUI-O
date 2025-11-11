import datetime
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from typing import Optional, Dict, Any

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Movemos a função _get_metas para cá
def _get_metas() -> Dict[str, Any]:
    """
    Retorna o dicionário de metas. No futuro, pode buscar do Supabase.
    """
    return {
        "dev_pdv_meta_perc": 2.64,
        "dev_pdv_meta": "2,64%", 
        "dev_pdv_premio": 160.00,
        "rating_meta_perc": 35.07,
        "rating_meta": "35,07%", 
        "rating_premio": 100.00,
        "refugo_meta_perc": 1.0,
        "refugo_meta": "1.0%", 
        "refugo_premio": 100.00
    }

@router.get("/metas")
async def ler_relatorio_metas(
    request: Request,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
):
    
    hoje = datetime.date.today()
    data_inicio = data_inicio or hoje.replace(day=1).isoformat()
    data_fim = data_fim or hoje.isoformat()
    
    metas = _get_metas() # Pega as metas

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "main_tab": "metas",
        "metas": metas,
        "data_inicio_selecionada": data_inicio,
        "data_fim_selecionada": data_fim,
        # Variáveis vazias para o template não falhar
        "view_mode": "equipas_fixas",
        "incentivo_tab": "motoristas",
        "search_query": "",
        "error_message": None,
        "resumo_viagens": [],
        "dashboard_equipas": [],
        "incentivo_motoristas": [],
        "incentivo_ajudantes": [],
    })