from azure.cosmos import CosmosClient, PartitionKey
from urllib.request import urlopen as u_req
from bs4 import BeautifulSoup as soup
import azure.functions as func
import logging
import json
import os

endpoint = os.environ['COSMOSDB_ENDPOINTURI']
key = os.environ['COSMOSDB_PRIMARYKEY']
database_name = 'library'
first_container_name = 'books'
second_container_name = 'categories'
base_url = 'https://books.toscrape.com/catalogue/'
categories_dict = {}

file = json.load(open('openapi.json', 'r'))


def main(req: func.HttpRequest) -> func.HttpResponse:
    action = req.params.get('action')
    if not action:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            action = req_body.get('action')

    if action == "start":
        logging.info('Python HTTP trigger function processed a request.')

        client = CosmosClient(endpoint, key)
        database = client.create_database_if_not_exists(id=database_name)
        ru_count = 0

        first_container = database.create_container_if_not_exists(
            id=first_container_name,
            partition_key=PartitionKey(path="/id"),
            offer_throughput=100
        )

        second_container = database.create_container_if_not_exists(
            id=second_container_name,
            partition_key=PartitionKey(path="/Category"),
            offer_throughput=100
        )

        # Loop over 200 books (20 at each page)
        for i in range(1, 11):
            bookshelf_url = base_url + 'page-' + str(i) + '.html'
            u_client = u_req(bookshelf_url)
            page_html = u_client.read()
            u_client.close()

            results = soup(page_html, "html.parser")

            bookshelf = results.findAll("h3")
            for book in bookshelf:
                u_client_book = u_req(base_url + book.a["href"])
                page_html = u_client_book.read()
                u_client_book.close()

                results = soup(page_html, "html.parser")

                book = results.find("table", {"class": "table table-striped"})
                book_title = results.find("div", {"class": "col-sm-6 product_main"}).h1.text
                book_code = book.findAll("tr")[0].td.text
                if results.find("ul", {"class": "breadcrumb"}).findAll("li")[2].a.text == "Add a comment":
                    book_category = "Unknown"
                else:
                    book_category = results.find("ul", {"class": "breadcrumb"}).findAll("li")[2].a.text
                book_price = book.findAll("tr")[2].td.text[1:]
                book_availability = book.findAll("tr")[5].td.text

                if book_category in categories_dict:
                    categories_dict[book_category].append({"id": book_code, "Title": book_title})
                else:
                    categories_dict[book_category] = [{"id": book_code, "Title": book_title}]

                book_json = {
                    'id': book_code,
                    'Title': book_title,
                    'Category': book_category,
                    'Price': book_price,
                    'Stock': book_availability
                }

                first_container.upsert_item(body=book_json)
                ru_count += float(first_container.client_connection.last_response_headers['x-ms-request-charge'])

        for category in categories_dict:
            category_json = {
                    "id": str(hash(category))[-10:],
                    "Category": category.replace(" ", "_"),
                    "Books": categories_dict[category]
                }
            second_container.upsert_item(body=category_json)
            ru_count += float(second_container.client_connection.last_response_headers['x-ms-request-charge'])

        books_query = "SELECT * FROM books"

        books_items = list(first_container.query_items(
            query=books_query,
            enable_cross_partition_query=True
        ))

        categories_query = "SELECT * FROM categories"

        categories_items = list(second_container.query_items(
            query=categories_query,
            enable_cross_partition_query=True
        ))

        return func.HttpResponse(
            '''Books query returned {0} items. Categories query returned {1} items.
            Population operation consumed {2} request units.'''.format(len(books_items),
                                                                       len(categories_items),
                                                                       ru_count),
            status_code=200
        )
    else:
        return func.HttpResponse(json.dumps(file), status_code=200)