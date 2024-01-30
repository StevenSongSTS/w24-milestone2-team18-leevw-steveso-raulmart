import argparse

from db_helper_functions import get_stock_news_from_db, update_stock_news_table
from benziga_article_scraper import scrape_article


def seed_news_articles(ticker):
    df = get_stock_news_from_db(ticker)

    for row in df.iterrows():
        id = row[1]["id"]
        url = row[1]["url"]
        article = row[1]["article"]

        if article:
            print(
                f"Row {row[0]}/{len(df)}, DB id={id} has already been scrapped, skipping entry."
            )
            continue

        print(f"Scraping row {row[0]}/{len(df)}, DB id={id}")

        article_text = scrape_article(url)
        update_stock_news_table(id, article_text)


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

    seed_news_articles(ticker)


if __name__ == "__main__":
    main()
