from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.board.models import category, announcement_card
from src.database import get_async_session

router = APIRouter(
    prefix="/board",
    tags=["Board"]
)


@router.get("/")
async def get_board_on_category(board_category: int, session: AsyncSession = Depends(get_async_session)):
    query = select(announcement_card).where(announcement_card.c.category_id == board_category)
    result = await session.execute(query)
    return result.mappings().all()