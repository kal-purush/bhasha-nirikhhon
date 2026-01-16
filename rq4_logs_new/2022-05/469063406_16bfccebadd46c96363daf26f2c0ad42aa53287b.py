import json
import requests
import pandas as pd
import credentials as c

URL = "https://ioproject-backend.herokuapp.com"
USERNAME = c.username
PASSWORD = c.password


def http_get_request(endpoint: str, headers: dict = None, payload: dict = None) -> requests.Response:
    return requests.get(f"{URL}/{endpoint}", headers=headers, params=payload)


def http_post_request(
        endpoint: str, headers: dict = None, payload: dict = None, data: dict = None
) -> requests.Response:
    return requests.post(f"{URL}/{endpoint}", headers=headers, json=payload, data=data)


def http_del_request(endpoint: str, headers: dict = None, payload: dict = None) -> requests.Response:
    return requests.delete(f"{URL}/{endpoint}", headers=headers, json=payload)


def login():
    login = json.loads(http_post_request(endpoint="login", data={"username": USERNAME, "password": PASSWORD}).text)
    return login["access_token"], login["token_type"]


def main():
    # Login
    access_token, token_type = login()
    headers = {"Authorization": token_type + " " + access_token}
    hair_products = pd.read_csv('./products_data/hair_products.csv', header=0)
    skin_products = pd.read_csv('./products_data/skincare_products.csv', header=0)
    # Create products
    for index, product in hair_products.iterrows():
        hair_product = {
            "product_name": product['name'],
            "product_brand": product['brand'],
            "product_type": product['type'],
            "straight": product['straight'],
            "wavy": product['wavy'],
            "curly": product['curly'],
            "coily": product['coily'],
            "fine": product['fine'],
            "medium": product['medium'],
            "thick": product['thick'],
            "image_link": product['img_link']
        }
        sample = http_post_request(endpoint="hairproduct", headers=headers, payload=hair_product)
        print(sample.text)
    # sample2 = http_get_request(endpoint="hairproduct", headers=headers)
    # print(sample2.text)

    for index, product in skin_products.iterrows():
        skin_product = {
            "product_name": product['name'],
            "product_brand": product['brand'],
            "product_type": product['type'],
            "oily": product['oily'],
            "dry": product['dry'],
            "normal": product['normal'],
            "image_link": product['img_link']
        }
        sample3 = http_post_request(endpoint="skinproduct", headers=headers, payload=skin_product)
        print(sample3.text)
    # sample4 = http_get_request(endpoint="skinproduct/800", headers=headers)
    # print(sample4.text)


if __name__ == "__main__":
    main()