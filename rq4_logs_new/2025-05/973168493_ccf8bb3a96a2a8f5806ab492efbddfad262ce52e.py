import logging
import nest_asyncio
from fastapi import FastAPI
from mangum import Mangum
from app.routers import vitibrasil

nest_asyncio.apply()

app = FastAPI(
    title="Vitibrasil API",
    description="API da Vitibrasil.",
    version="1.0.0"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message).200s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
    handlers=[
        logging.StreamHandler()
    ]
)

app.include_router(vitibrasil.router)


handler = Mangum(app)