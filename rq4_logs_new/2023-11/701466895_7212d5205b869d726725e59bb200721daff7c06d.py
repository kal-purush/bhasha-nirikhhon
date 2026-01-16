from fastapi import FastAPI
from models.store import (
    view_store,
    add_new_product_in_store,
    change_quantity_product_in_store,
)
from models.cart import add_product, delete_product


from models.login import login_seller, login_customer
import uvicorn
from utils import User, Product


app = FastAPI()


@app.get("/store")
def view_store_route():
    return view_store()


@app.post("/logins/")
def login_seller_route(seller: User):
    return login_seller(seller)


@app.post("/loginc/")
def login_customer_router(customer: User):
    return login_customer(customer)


@app.post("/add_product_in_store")
def add_new_product_in_store_route(seller: User, product: Product):
    return add_new_product_in_store(seller, product)


@app.post("/change_quantity")
def change_quantity_product_in_store_route(seller: User, product: Product):
    return change_quantity_product_in_store(seller, product)


@app.post("/add_cart")
def add_product_route(kupac: User, product: Product):
    return add_product(kupac, product)

@app.post("/delete_product")
def delete_product_route(kupac: User, product: Product):
    return delete_product(kupac, product)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)