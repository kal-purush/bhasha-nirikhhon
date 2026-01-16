from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.category.models import category
from src.category.schemas import CategoryCreate
from src.database import get_async_session

router = APIRouter(
    prefix="/category",
    tags=["Category"]
)


@router.get("/")
async def get_category(category_id: int, session: AsyncSession = Depends(get_async_session),
                                limit: int = 3, offset: int = 0):
    try:
        query = select(category).where(category.c.id == category_id)
        result = await session.execute(query)
        return {
            "status": "success",
            "data": result.mappings().all()[offset:][:limit],
            "details": None
        }
    except Exception:
        raise HTTPException(status_code=500, detail={
            "status": "error",
            "data": None,
            "details": None
        })


@router.post("/")
async def post_board(new_card: CategoryCreate, session: AsyncSession = Depends(get_async_session)):
    try:
        stmt = insert(category).values(**new_card.model_dump())
        await session.execute(stmt)
        await session.commit()
        return {
            "status": 200
        }
    except Exception:
        raise HTTPException(status_code=500, detail={
            "status": "category not save",
            "data": None,
            "details": None
        })