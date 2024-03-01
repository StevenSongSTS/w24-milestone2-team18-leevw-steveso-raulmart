import pandas as pd
from constants import DB_URL
from db_helper_functions import (
    create_bertopic_sentiment_scores_table,
    drop_bertopic_sentiment_scores_table,
)


def seed_scores():

    df = pd.read_csv("./bertopic_sentiment_score.csv")

    df[["fk_stock_news_id", "positive", "negative", "neutral"]].to_sql(
        "bertopic_sentiment_scores", DB_URL, if_exists="append", index=False
    )


def main() -> None:
    """
    Orchestrates program flow.

    Parameters:
        None
    Returns:
        None
    """
    drop_bertopic_sentiment_scores_table()
    create_bertopic_sentiment_scores_table()
    seed_scores()


if __name__ == "__main__":
    main()
