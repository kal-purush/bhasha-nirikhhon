from typing import Optional

from pydantic import BaseModel, Field


class CoinSchema(BaseModel):
    name: str = Field(...)
    symbol: str = Field(..., max_length=5)
    qty: float = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "name": "Bitcoin",
                "symbol": "BTC",
                "qty": 0.256
            }
        }


class UpdateCoinModel(BaseModel):
    name: Optional[str]
    symbol: Optional[str]
    qty: Optional[str]

    class Config:
        schema_extra = {
            "example": {
                "name": "Bitcoin",
                "symbol": "BTC",
                "qty": 0.36
            }
        }
