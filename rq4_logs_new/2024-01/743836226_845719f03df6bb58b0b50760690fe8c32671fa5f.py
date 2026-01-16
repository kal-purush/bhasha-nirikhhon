from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates

from src.board.router import get_board_on_category
from src.category.router import get_all_category, get_category

router = APIRouter(
    prefix="/pages",
    tags=["Pages"],
)

templates = Jinja2Templates(directory="src/templates")

@router.get("/home")
def get_base_page(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})


@router.get("/category/")
def get_details_page(request: Request, categories=Depends(get_all_category)):
    return templates.TemplateResponse("category.html", {"request": request, "categories": categories["data"]})

@router.get("/search/{title_category}")
def get_details_page(request: Request, cards=Depends(get_board_on_category)):
    return templates.TemplateResponse("search.html", {"request": request, "cards": cards["data"]})