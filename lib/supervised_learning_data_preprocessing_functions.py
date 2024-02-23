import pandas as pd
import yfinance
from constants import DATA_END_DATE, DATA_START_DATE


def gen_df_for_supervised_learning(ticker: str, sentiment_df_retrieval_function):
    sentiment_articles_df = sentiment_df_retrieval_function(ticker)
    grouped_sentiments = (
        sentiment_articles_df.groupby("date", as_index=False)
        .agg({"positive": "mean", "negative": "mean", "neutral": "mean"})
        .sort_values(by="date", ascending=True)
    )
    stock_price_history = (
        yfinance.Ticker(ticker)
        .history(start=DATA_START_DATE, end=DATA_END_DATE)
        .reset_index()
    )
    stock_price_history.columns = [
        "_".join(x.lower().split(" ")) for x in stock_price_history.columns
    ]
    stock_price_history["date"] = stock_price_history["date"].dt.date

    combo_df = pd.merge(
        stock_price_history,
        grouped_sentiments,
        left_on="date",
        right_on="date",
        how="left",
    )

    combo_df["date"] = pd.to_datetime(combo_df["date"])
    combo_df = combo_df.sort_values(by="date", ascending=True)
    combo_df = combo_df.set_index("date")
    combo_df["day_of_month"] = combo_df.index.day
    combo_df["day_of_week"] = combo_df.index.dayofweek
    combo_df["quarter"] = combo_df.index.quarter
    combo_df["month"] = combo_df.index.month
    combo_df["year"] = combo_df.index.year
    combo_df["month_year"] = combo_df.index.to_period("M")
    combo_df["week_year"] = combo_df.index.to_period("W")
    combo_df[["positive", "negative", "neutral"]] = combo_df[
        ["positive", "negative", "neutral"]
    ].ffill()
    combo_df[["positive", "negative", "neutral"]] = combo_df[
        ["positive", "negative", "neutral"]
    ].shift(1)
    combo_df[["prev_high", "prev_low", "prev_close", "prev_volume"]] = combo_df[
        ["high", "low", "close", "volume"]
    ].shift(1)
    combo_df = combo_df.iloc[1:]

    return combo_df
