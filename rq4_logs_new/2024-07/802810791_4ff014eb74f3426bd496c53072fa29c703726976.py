from pydantic import BaseModel, field_validator

from models.products import Products


class Event(BaseModel):
    operation: str
    product: str
    quantity: int

    @field_validator('product')
    def validate_product(cls, value):
        product = Products.objects.get(id=value, status=True, is_deleted=False)
        return product