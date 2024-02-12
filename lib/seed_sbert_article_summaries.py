import argparse

import numpy as np
from constants import DB_URL, MAX_TEXT_SUMMARY_LEN, MIN_TEXT_SUMMARY_LEN
from db_helper_functions import (
    get_stock_news_from_db,
)
from summarizer.sbert import SBertSummarizer


def seed_text_summaries(ticker: str):

    df = get_stock_news_from_db(ticker)
    df = df.rename(columns={"id": "fk_stock_news_id"})
    df = df[~df.article.isnull()]
    df["article"] = df["article"].apply(
        lambda x: x.replace("\xa0", " ").replace("Loading...", "")
    )

    sbert_summarizer = SBertSummarizer("paraphrase-MiniLM-L6-v2")

    for _idx, chunk in df.groupby(np.arange(len(df)) // 100):

        chunk["summary"] = chunk["article"].apply(
            lambda x: "".join(
                sbert_summarizer(
                    x, min_length=MIN_TEXT_SUMMARY_LEN, max_length=MAX_TEXT_SUMMARY_LEN
                )
            )
        )

        chunk[["fk_stock_news_id", "summary"]].to_sql(
            "stock_news_summaries", DB_URL, if_exists="append", index=False
        )


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
            "Please include ticker symbol of the company you would like to generate summaries for, as a commandline argument. Example: -ticker=MSFT"
        )
        return

    seed_text_summaries(ticker)


if __name__ == "__main__":
    main()
