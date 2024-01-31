import requests
from bs4 import BeautifulSoup
import time


def scrape_article(url) -> str:
    """ """
    page = requests.get(url)
    status_code = page.status_code
    if status_code == 429:
        time.sleep(30)
    soup = BeautifulSoup(page.content, "html.parser")
    article = soup.find(id="article-body")
    return article.text
