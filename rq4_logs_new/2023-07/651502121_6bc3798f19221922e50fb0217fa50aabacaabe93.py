from scrapy.http.response.html import HtmlResponse
from re import search
from scrapy.selector.unified import Selector


def convert_discount_to_number(discount_string: str) -> int:
    discount = search("\d{1,2}%", discount_string)
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