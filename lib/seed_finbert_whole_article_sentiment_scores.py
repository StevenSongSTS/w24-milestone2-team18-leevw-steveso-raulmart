import argparse

import numpy as np
import torch
from constants import DB_URL
from db_helper_functions import (
    create_finbert_whole_article_sentiment_scores_table,
    drop_finbert_whole_article_sentiment_scores_table,
    get_finbert_whole_article_news_ids,
    get_stock_news_from_db,
)
import gc
from nltk.tokenize import sent_tokenize
from transformers import AutoModelForSequenceClassification, AutoTokenizer

finbert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
finbert_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")


def gen_sentiments(sent_list):
    embedding = finbert_tokenizer(
        sent_list, padding=True, return_tensors="pt", truncation=True
    )
    outputs = finbert_model(**embedding)
    predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
    return np.round(np.mean(predictions.tolist(), axis=0), 4)


def iterative_sentiment_predictions(ticker: str):
    existing_scores = get_finbert_whole_article_news_ids()["fk_stock_news_id"].to_list()

    df = get_stock_news_from_db(ticker)
    df = df.rename(columns={"id": "fk_stock_news_id"})
    df = df[~df.article.isnull()]
    df = df[~df.fk_stock_news_id.isin(existing_scores)]

    df["article"] = df["article"].apply(
        lambda x: x.replace("\xa0", " ").replace("\n", "").replace("Loading...", "")
    )
    df["article"] = df["article"].apply(lambda x: sent_tokenize(x))

    groups = df.groupby(np.arange(len(df)) // 1)

    for idx, chunk in groups:
        print(f"iteration {idx+1}/{len(groups)}")
        article_length = len(chunk["article"].iloc[0])
        if article_length >= 100:
            print(
                f"SKIPPING: iteration {idx+1}/{len(groups)}: REASON: article size == {article_length}"
            )
            continue

        chunk[["positive", "negative", "neutral"]] = chunk.apply(
            lambda x: gen_sentiments(x["article"]), axis="columns", result_type="expand"
        )

        chunk[["fk_stock_news_id", "positive", "negative", "neutral"]].to_sql(
            "finbert_whole_article_sentiment_scores",
            DB_URL,
            if_exists="append",
            index=False,
        )
        gc.collect()


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

    # drop_finbert_whole_article_sentiment_scores_table()
    # create_finbert_whole_article_sentiment_scores_table()

    parsed_args = parser.parse_args()
    ticker = parsed_args.ticker

    if not ticker:
        print(
            "Please include ticker symbol of the company you would like to generate sentiment scores for, as a commandline argument. Example: -ticker=MSFT"
        )
        return

    iterative_sentiment_predictions(ticker)


if __name__ == "__main__":
    main()
