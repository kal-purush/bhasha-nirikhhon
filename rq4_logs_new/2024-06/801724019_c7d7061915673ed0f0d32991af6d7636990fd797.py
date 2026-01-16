import logging

from common.data_handlers.search_handler import SearchHandler
from fastapi import APIRouter, HTTPException
from models.search import Search

router = APIRouter()
logger = logging.getLogger("uvicorn.error")
search_handler = SearchHandler()


@router.get("", response_model=Search)
async def search(search_query: str):
    try:
        return search_handler.search(search_query)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=f"Failed to search with error: {e}")