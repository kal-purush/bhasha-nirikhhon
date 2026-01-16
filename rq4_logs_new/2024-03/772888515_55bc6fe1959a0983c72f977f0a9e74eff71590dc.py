from fastapi import APIRouter, Depends, HTTPException

from app.api.v1.endpoints.auth import get_current_user

from app.schemas.message_schemas import MSGIn
from app.services.exceptions import handle_domain_error
from app.services.use_case.index import get_file_by_id

router = APIRouter()


@router.get("/message")
@handle_domain_error
async def get_user_orders(payload: MSGIn = Depends(), user=Depends(get_current_user)) -> str:
    # order = await get_order_by_id(payload.order_id)
    # if order.status != OrderStatus.done:
    #     raise HTTPException(status_code=status.HTTP_425_TOO_EARLY, detail="Заказ еще обрабатывается")
    file = get_file_by_id(payload.order_id)
    text = "Какое то текст"
    highlights = ["Сделай ....", "Выполни ..."]
    persons = ["Ваня В", "Кирилл", "Дмитрий"]
    summary = [{}]
    if file:
        file = file[0]
        source = file["_source"]
        text = source.get("content")
        persons = source.get('persons')
        summary = source.get('summary')
        highlights = file.get("highlight")
        if highlights:
            highlights = highlights['content']

    if payload.message == "Какие были поручения?":
        return "\n".join(highlights)

    elif payload.message == "О чем был текст?":
        for summ in summary:
            if summ.get("name") == "All":
                return summ.get("text", "")

    elif payload.message == "Кто что сказал?":
        res = []
        for summ in summary:
            if summ.get("name") != "All":
                res.append(f"{summ.get("name", "Кто то")}: \n {summ.get("text", "")}")
        return "\n".join(res)

    elif payload.message == "Кто присутствовал на встрече?":
        return "\n".join(highlights)

    return "Я не знаю такую команду"