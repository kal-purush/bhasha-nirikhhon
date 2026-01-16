from aiohttp import ClientSession
from typing import Dict
from bs4 import BeautifulSoup


Metatags = Dict[str, str | None]


async def extract_meta_tags_open_graph(url: str) -> Metatags:
    metatags: Metatags = {}
    async with ClientSession() as session:
        async with session.get(url) as res:
            content = await res.text()
            soup = BeautifulSoup(content, 'html.parser')

            title = soup.find("title")
            description = soup.find("meta", attrs={"name": "description"})

            og_title = soup.find("meta", property="og:title")
            og_type = soup.find("meta", property="og:type")
            og_url = soup.find("meta", property="og:url")
            og_description = soup.find("meta", property="og:description")
            og_image = soup.find("meta", property="og:image")

            metatags["title"] = title.text.strip() if title else None
            metatags["description"] = description["content"] if description else None

            metatags["og_title"] = og_title["content"] if og_title else None
            metatags["og_type"] = og_type["content"] if og_type else None
            metatags["og_url"] = og_url["content"] if og_url else None
            metatags["og_description"] = og_description["content"] if og_description else None
            metatags["og_image"] = og_image["content"] if og_image else None

    return metatags