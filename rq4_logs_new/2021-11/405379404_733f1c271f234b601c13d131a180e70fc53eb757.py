from pydantic import BaseModel

class MyIngredientsUpdateRequest(BaseModel):
    recipe_id: str
    user_id: str
    ingredients: list[str]