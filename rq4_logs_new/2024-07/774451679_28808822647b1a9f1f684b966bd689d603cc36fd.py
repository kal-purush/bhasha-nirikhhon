"""This script going to scrape information from MAD database (https://mad.ibcp.fr/explore).
This page contains the common name, the alias, the category and the link to the page of the lipid.

Usage
-----
    python scrap_MAD.py
"""

__authors__ = ("Karine DUONG", "Pierre POULAIN")
__contact__ = "pierre.poulain@u-paris.fr"


import pandas as pd
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import requests


async def scrape_with_playwright(url: str) -> str:
    """Scrapes the HTML content of a webpage using Playwright.

    Parameters
    ----------
        url: str
            The URL of the webpage to be scraped.

    Returns
    -------
        str
            The HTML content of the specified webpage.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url)
        await page.wait_for_selector("tbody")
        html_content = await page.content()
        await browser.close()

        return html_content


async def main_playwright(url: str) -> BeautifulSoup:
    """Fetches and parses the HTML content of a webpage using Playwright and BeautifulSoup.

    Parameters
    ----------
        url: str
            The URL of the webpage to be scraped.

    Returns
    -------
        BeautifulSoup
            A BeautifulSoup object representing the parsed HTML content of the webpage.
    """
    html_content = await scrape_with_playwright(url)
    soup = BeautifulSoup(html_content, "html.parser")
    return soup


def is_max(soup: BeautifulSoup) -> bool:
    """Check if the current page is the last page of results.

    Parameters
    ----------
        soup: BeautifulSoup)
            A BeautifulSoup object containing the parsed HTML
            of the current page.

    Returns
    -------
        bool
            True if the current page is the last page of results, False otherwise.
    """
    rows = soup.find("tfoot").find_all("tr")
    for row in rows:
        cell = row.find("p")
        current_page = cell.get_text()
        tmp = current_page.split()
        tot_elmt_now = int(tmp[0].split("-")[1])
        tot_elmt = int(tmp[-1])
        return tot_elmt_now == tot_elmt


def parse_MAD_one(page: int, recordings: list[dict[str, str]]) -> list[dict[str, str]]:
    """Parse data from the MAD website for a single page.

    Parameters
    ----------
        page: int
            The page number to parse.
        recordings: list[dict[str, str]]
            A list of dictionaries containing previously parsed recordings.

    Returns
    -------
        list[dict[str, str]]
            The updated list of recordings after parsing the specified page.
    """
    url = f"https://mad.ibcp.fr/explore?page={page}"
    loop = asyncio.get_event_loop()
    soup = loop.run_until_complete(main_playwright(url))

    rows = soup.find("tbody").find_all("tr")
    base_url = "https://mad.ibcp.fr"
    col_names = [col.get_text() for col in soup.select("th")]
    col_names.append("Lien")

    for row in rows:
        cells = row.find_all("td")
        link = row.find("a")
        link_href = base_url + link.get("href")
        recording = {
            name: value.get_text(strip=True)
            for name, value in zip(col_names, cells)
            if name != "Created at"
        }
        recording["Lien"] = link_href
        recordings.append(recording)

    if not is_max(soup):
        parse_MAD_one(page + 1, recordings)
    return recordings


def parse_MAD_loop() -> pd.core.frame.DataFrame:
    """Parse data from the MAD website for multiple pages using a loop.

    Returns
    -------
        pd.core.frame.DataFrame
            DataFrame containing the parsed data from the MAD website.
    """
    recordings = []
    data = parse_MAD_one(1, recordings)
    df = pd.DataFrame(data)
    df.to_csv(
        "lipid_MAD.csv", sep=",", quotechar='"', index=False, header=True, columns=df.columns.tolist()
    )
    return df


if __name__ == "__main__":
    nest_asyncio.apply()  # permet de créer une autre boucle d'évent, dans un env qui en a déja un en cours d'éxécution
    parse_MAD_loop()