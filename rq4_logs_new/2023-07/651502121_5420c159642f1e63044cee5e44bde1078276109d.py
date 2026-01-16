from typing import Generator
from logging import getLogger

logger = getLogger("get_generator.py")


def get_deals_pages_generator(
    deals_pages_count: int, invert: bool = False
) -> Generator[str, None, None]:
    first_page = "https://www.amazon.com.br/deals?deals-widget=%257B%2522version%2522%253A1%252C%2522viewIndex%2522%253A0%252C%2522presetId%2522%253A%2522deals-collection-all-deals%2522%252C%2522sorting%2522%253A%2522FEATURED%2522%257D"
    url_format = lambda first_deals_page, current_page_number: first_deals_page.replace(
        "%253A0%", f"%253A{current_page_number * 3}0%"
    )
    if invert:
        return (
            url_format(first_page, i) for i in range(deals_pages_count * 2 - 1, -1, -1)
        )
    return (url_format(first_page, i) for i in range(0, deals_pages_count * 2 - 1))