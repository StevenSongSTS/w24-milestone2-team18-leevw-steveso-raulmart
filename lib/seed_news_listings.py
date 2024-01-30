import argparse

from benzinga import news_data

from lib.constants import BENZINGA_KEY
from lib.db_helper_functions import (
    create_stock_news_table,
    drop_stock_news_table,
    insert_into_stock_news_table,
)

news = news_data.News(BENZINGA_KEY)


def news_api_call(page, ticker):
    return news.news(
        pagesize=100,
        page=page,
        date_from="2019-01-04",
        date_to="2023-01-04",
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
    parser.add_argument("-reset_database_table", help="Do the bar option")
    parsed_args = parser.parse_args()
    ticker = parsed_args.ticker
    reset_database_table = parsed_args.reset_database_table

    if not ticker:
        print(
            "Please include ticker symbol of the company you would like to seed as a commandline argument. Example: -ticker=MSFT"
        )
        return

    if reset_database_table:
        print("Resetting Database Tables...")
        drop_stock_news_table()
        create_stock_news_table()

    seed_news_article_links(ticker)


if __name__ == "__main__":
    main()
