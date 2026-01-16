from bs4 import BeautifulSoup
from bs4.element import Tag
import re


def get_title(soup: BeautifulSoup):
    assert soup.title.string is not None, "title must not be None"
    return re.sub(r"[^a-zA-Zà-úÀ-Ú0-9_\s\./]", "", soup.title.string)


def get_reviews(soup: BeautifulSoup):
    title = soup.find(id="acrCustomerReviewText")
    if title is None:
        return 0
    reviews = re.search("^[\d*\.]+", title.get_text())
    if not reviews:
        return 0
    return int(reviews.group().replace(".", ""))


def get_category(soup: BeautifulSoup, delimiter: str = " > "):
    element = soup.find(id="wayfinding-breadcrumbs_container")
    if element is None:
        return "Not Defined"
    element = str(element)
    inner_text = re.findall(r">\n([\s/\n\w]+)<", element)
    if inner_text:
        return delimiter.join(
            set([txt.strip() for txt in inner_text if txt.strip() != ""])
        )
    return "Not Defined"


def get_is_prime(soup: BeautifulSoup) -> str:
    prime_div = soup.find(id="primeSavingsUpsellCaption_feature_div")
    if prime_div is not None:
        return "true"

    prime_div = soup.select(
        "div.tabular-buybox-text:nth-child(4) div:nth-child(1) span:nth-child(1)"
    )
    if prime_div:
        prime_div = prime_div[0].text
        if "amazon" in prime_div.strip().lower():
            return "true"

    prime_div = soup.select(
        "div#mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE span"
    )
    if prime_div:
        prime_div = prime_div[0].text
        if "grátis" in prime_div.strip().lower():
            return "true"

    return "false"


def convert_price_to_number(price_string: str) -> float:
    price = re.search("[\d\,\.]+", price_string)
    return float(price.group().replace(".", "").replace(",", "."))


def loop_price_selectors(element: Tag):
    """
    Com o element selecionado e existente, roda um loop nos possíveis seletores
    que podem possuir o preço, e se esse seletor existir, retorna o conteúdo (innerText)
    do seletor.
    """
    possible_price_selectors = ["span.a-offscreen"]
    for selector in possible_price_selectors:
        has_content = element.select(selector)
        if has_content:
            return has_content[0].text
    return None


def get_price(soup: BeautifulSoup) -> float:
    price_container_element = soup.find(id="corePrice_feature_div")
    if price_container_element:
        price = loop_price_selectors(price_container_element)
        if price:
            return convert_price_to_number(price)
    del price_container_element

    # no caso de livros, #corePrice_feature_div existe, mas não contem dados relevantes, o elemento a ser capturado é outro
    book_price = soup.find(id="price")
    if book_price:
        return convert_price_to_number(book_price.text)
    del book_price

    # para ebooks kindle, o processo é semelhante ao de livros
    ebook_price = soup.find(id="kindle-price")
    if ebook_price:
        return convert_price_to_number(ebook_price.text)

    return 0.0


def convert_discount_to_number(discount_string: str) -> int:
    discount = re.search("\d{1,2}%", discount_string)
    return int(discount.group().replace("%", ""))


def get_discount(soup: BeautifulSoup) -> int:
    discount_basic_element = soup.find("span", {"class": "savingPriceOverride"})
    if discount_basic_element:
        return convert_discount_to_number(discount_basic_element.text)
    del discount_basic_element

    # livros fisicos
    book_discount = soup.find(id="savingsPercentage")
    if book_discount:
        return convert_discount_to_number(book_discount.text)
    del book_discount

    # ebooks
    ebook_discount = soup.select("p.ebooks-price-savings")
    if ebook_discount:
        return convert_discount_to_number(ebook_discount[0].text)
    del ebook_discount

    # preços e descontos em <table>
    table_discount = soup.select(
        "tr td.a-span12.a-color-price.a-size-base span.a-color-price"
    )
    if table_discount:
        return convert_discount_to_number(table_discount[0].text)

    return 0


def get_item(pid: str, soup: BeautifulSoup) -> dict:
    item = {}
    item["id"] = pid
    item["title"] = get_title(soup)
    item["category"] = get_category(soup)
    item["reviews"] = get_reviews(soup)
    item["is_prime"] = get_is_prime(soup)
    item["price"] = get_price(soup)
    item["discount"] = get_discount(soup)
    return item