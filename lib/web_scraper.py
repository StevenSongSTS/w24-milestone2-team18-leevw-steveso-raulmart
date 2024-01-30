from typing import List, Dict
import requests
from bs4 import BeautifulSoup


def scrape_article(url) -> str:
    """ """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    article = soup.find(id="article-body")
    return article.text
