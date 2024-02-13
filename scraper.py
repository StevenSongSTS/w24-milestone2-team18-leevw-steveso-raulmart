import requests
from bs4 import BeautifulSoup


def scrape(url:str) -> str: 
    # Send a GET request to the URL
    response = requests.get(url)

    # Create a bs4 object
    soup = BeautifulSoup(response.content, 'html.parser')

    # Get the article content from the 'article-body' div
    article_body = soup.find('div', id='article-body')

    # Get all childeren divs 
    child_divs = article_body.find_all('div')

    # Based on the Benzinga html convention, article content always apears in the first div
    content_div = child_divs[0]

    # Get all the p HTML elements out of the div
    content_p = content_div.find_all('p')

    # Filter out the 'Read Next' section
    extracted_content = '\n'.join(p.get_text() for p in content_p[:-2])

    return extracted_content


url = 'https://www.benzinga.com/news/24/01/36837827/trump-says-record-stock-market-surge-fueled-by-investors-betting-on-his-return-to-white-house-my-pol'
print(scrape(url))