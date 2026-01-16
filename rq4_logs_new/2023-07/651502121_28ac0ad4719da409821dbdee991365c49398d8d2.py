from scrapy.http.response.html import HtmlResponse
from scrapy.selector.unified import Selector
import re
import json


def get_title(response: HtmlResponse) -> str:
    title = response.css("#productTitle::text").get()
    if title:
        return title.strip()
    return "Not Defined"


def get_biggest_image(element: Selector):
    images = element.css("img::attr(data-a-dynamic-image)").get()
    if images:
        images = list(json.loads(images).keys())
        return images[-1]
    return None


def get_image_url(response: HtmlResponse) -> str:
    # v1
    image_element = response.css("img.a-dynamic-image::attr(data-old-hires)").get()
    if image_element:
        return image_element
    del image_element

    # v2
    image_element = response.css("#landingImage::attr(data-old-hires)").get()
    if image_element:
        return image_element
    del image_element

    # v3
    image_element = response.css("#landingImage")
    if image_element:
        image_url = get_biggest_image(image_element[0])
        if image_url:
            return image_url

    # livros físicos
    image_element = response.css("#ebooksImgBlkFront")
    if image_element:
        image_url = get_biggest_image(image_element[0])
        if image_url:
            return image_url
    del image_element

    # livros físicos v2
    image_element = response.css("#imgBlkFront")
    if image_element:
        image_url = get_biggest_image(image_element[0])
        if image_url:
            return image_url
    del image_element

    return "https://raw.githubusercontent.com/guimassoqueto/mocks/main/images/404.webp"


def get_previous_price(response: HtmlResponse):
    basis_price_element = response.css("span.basisPrice")
    if basis_price_element:
        previous_price = basis_price_element[0].css("span.a-offscreen::text").get()
        if previous_price:
            return convert_price_to_number(previous_price)
    del basis_price_element

    # ebooks
    basis_price_element = response.css("#digital-list-price::text").get()
    if basis_price_element:
        return convert_price_to_number(basis_price_element)
    del basis_price_element

    # livros físicos
    basis_price_element = response.css("#listPrice::text").get()
    if basis_price_element:
        return convert_price_to_number(basis_price_element)
    del basis_price_element

    return 0.0


def get_category(element: str | None, delimiter: str = " > ") -> str:
    if element is None:
        return "Not Defined"

    inner_text = re.findall(r">\n([\s/\n\w]+)<", element)
    if inner_text:
        return delimiter.join(
            set([txt.strip() for txt in inner_text if txt.strip() != ""])
        )

    return "Not Defined"


def convert_discount_to_number(discount_string: str) -> int:
    discount = re.search("\d{1,2}%", discount_string)
    return int(discount.group().replace("%", ""))


def get_discount(response: HtmlResponse) -> int:
    discount_basic_element = response.css("span.savingPriceOverride")
    if discount_basic_element:
        return convert_discount_to_number(discount_basic_element[0].get())
    del discount_basic_element

    # livros fisicos
    book_discount = response.css("#savingsPercentage::text").get()
    if book_discount:
        return convert_discount_to_number(book_discount)
    del book_discount

    # ebooks
    ebook_discount = response.css("p.ebooks-price-savings::text")
    if ebook_discount:
        return convert_discount_to_number(ebook_discount[0].get())
    del ebook_discount

    # preços e descontos em <table>
    table_discount = response.css(
        "tr td.a-span12.a-color-price.a-size-base span.a-color-price"
    )
    if table_discount:
        return convert_discount_to_number(table_discount[0].get())

    return 0


def get_free_shipping(response: HtmlResponse) -> str:
    prime_div = response.css("#primeSavingsUpsellCaption_feature_div").get()
    if prime_div is not None:
        return "true"

    prime_div = response.css(
        "div.tabular-buybox-text:nth-child(4)>div:nth-child(1)>span:nth-child(1)::text"
    ).get()
    if prime_div is not None and "amazon" in prime_div.strip().lower():
        return "true"

    prime_div = response.css(
        "#mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE>span::text"
    ).get()
    if prime_div is not None and "grátis" in prime_div.strip().lower():
        return "true"

    return "false"


def convert_price_to_number(price_string: str) -> float:
    price = re.search(r"[\d\,\.]+", price_string)
    return float(price.group().replace(".", "").replace(",", "."))


def loop_price_selectors(element: Selector):
    """
    Com o element selecionado e existente, roda um loop nos possíveis seletores
    que podem possuir o preço, e se esse seletor existir, retorna o conteúdo (innerText)
    do seletor.
    """
    possible_price_selectors = ["span.a-offscreen"]
    for selector in possible_price_selectors:
        has_content = element.css(f"{selector}::text").get()
        if has_content:
            return has_content
    return None


def get_price(response: HtmlResponse) -> str:
    price_container_element = response.css("#corePrice_feature_div")
    if price_container_element:
        price = loop_price_selectors(price_container_element[0])
        if price:
            return convert_price_to_number(price)
    del price_container_element

    # no caso de livros, #corePrice_feature_div existe, mas não contem dados relevantes, o elemento a ser capturado é outro
    book_price = response.css("#price::text").get()
    if book_price:
        return convert_price_to_number(book_price)
    del book_price

    # para ebooks kindle, o processo é semelhante ao de livros
    ebook_price = response.css("#kindle-price::text").get()
    if ebook_price:
        return convert_price_to_number(ebook_price)

    return 0.0