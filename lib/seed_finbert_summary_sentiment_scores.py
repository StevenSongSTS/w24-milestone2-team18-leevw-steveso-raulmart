import argparse

import numpy as np
import torch
from constants import DB_URL
from db_helper_functions import (
    get_news_summaries_from_db,
)
from transformers import AutoModelForSequenceClassification, AutoTokenizer


def iterative_sentiment_predictions(ticker: str):
    finbert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    finbert_model = AutoModelForSequenceClassification.from_pretrained(
        "ProsusAI/finbert"
    )

    df = get_news_summaries_from_db(ticker)
    df = df.rename(columns={"id": "fk_stock_news_id"})
    df = df[~df.summary.isnull()]

    for _idx, chunk in df.groupby(np.arange(len(df)) // 10):

        finbert_embeddings = finbert_tokenizer(
            chunk.summary.tolist(), padding=True, truncation=True, return_tensors="pt"
        )

        outputs = finbert_model(**finbert_embeddings)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        chunk[["positive", "negative", "neutral"]] = np.round(predictions.tolist(), 4)

        chunk[["fk_stock_news_id", "positive", "negative", "neutral"]].to_sql(
            "finbert_summary_sentiment_scores", DB_URL, if_exists="append", index=False
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
            "Please include ticker symbol of the company you would like to generate sentiment scores for, as a commandline argument. Example: -ticker=MSFT"
        )
        return

    iterative_sentiment_predictions(ticker)


if __name__ == "__main__":
    main()
