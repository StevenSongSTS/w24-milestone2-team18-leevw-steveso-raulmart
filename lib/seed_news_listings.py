import argparse

from benzinga import news_data

from constants import BENZINGA_KEY, DATA_END_DATE, DATA_START_DATE
from db_helper_functions import (
    insert_into_stock_news_table,
)

news = news_data.News(BENZINGA_KEY)


def news_api_call(page, ticker):
    return news.news(
        pagesize=100,
        page=page,
        date_from=DATA_START_DATE,
        date_to=DATA_END_DATE,
        company_tickers=ticker,
    )


def seed_news_article_links(ticker):
    page_num = 0
    articles_returned = 1

    while articles_returned > 0:
        articles = news_api_call(page_num, ticker)
        articles_returned = len(articles)
        page_num += 1
        insert_into_stock_news_table(articles, ticker)


def main() -> None:
    """
    Orchestrates program flow.

    Parameters:
        None
    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ticker",
        help="Ticker Symbol of company you would like to seed the database with",
    )
    parsed_args = parser.parse_args()
    ticker = parsed_args.ticker

    if not ticker:
        print(
            "Please include ticker symbol of the company you would like to seed as a commandline argument. Example: -ticker=MSFT"
        )
        return

    seed_news_article_links(ticker)


if __name__ == "__main__":
    main()
