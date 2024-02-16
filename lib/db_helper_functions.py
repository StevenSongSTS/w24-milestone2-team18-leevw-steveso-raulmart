from typing import Dict, List
import pandas as pd

import psycopg2
from constants import DB_URL


def connect_with_database() -> psycopg2.extensions.connection:
    """
    this function connects to the database and returns a connection to the database

    Parameters:
        None
    Returns:
        psycopg2.extensions.connection: returns a connection to the database
    """
    db_connection = psycopg2.connect(DB_URL)
    return db_connection


def sql_execution_wrapper(sql_statement: str) -> None:
    """
    this is a wrapper function to handle the execution of sql statements.
    It will connect to the database.
    Execute the sql statement.
    Commit the changes.
    Then close the connection.

    Parameters:
        sql_statement (str): the sql statement to execute
    Returns:
        psycopg2.extensions.connection: returns a connection to the database
    """
    conn = connect_with_database()
    cur = conn.cursor()

    cur.execute(sql_statement)

    conn.commit()
    cur.close()
    conn.close()


def create_stock_news_table() -> None:
    """
    This function creates the stock_news table in the database if it does not exist.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """
                CREATE TABLE IF NOT EXISTS stock_news(
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    title TEXT,           
                    url TEXT,           
                    article TEXT,                             
                    date DATE NOT NULL 
                    UNIQUE(title, date)
                );
                """
    sql_execution_wrapper(sql_query)


def create_stock_news_summaries_table() -> None:
    """
    This function creates the stock_news_summaries table in the database if it does not exist.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """
                CREATE TABLE IF NOT EXISTS stock_news_summaries(
                    id SERIAL PRIMARY KEY,
                    fk_stock_news_id INT UNIQUE,
                    summary TEXT,                   
                    CONSTRAINT fk_stock_news
                        FOREIGN KEY(fk_stock_news_id) 
                            REFERENCES stock_news(id)
                            ON DELETE CASCADE
                );
                """
    sql_execution_wrapper(sql_query)


def drop_stock_news_summaries_table() -> None:
    """
    This function deletes the stock_news_summaries table in the database if it exists.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """DROP TABLE IF EXISTS stock_news_summaries;"""
    sql_execution_wrapper(sql_query)


def create_finbert_summary_sentiment_scores_table() -> None:
    """
    This function creates the finbert_summary_sentiment_scores table in the database if it does not exist.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """
                CREATE TABLE IF NOT EXISTS finbert_summary_sentiment_scores(
                    id SERIAL PRIMARY KEY,
                    fk_stock_news_id INT UNIQUE,
                    positive FLOAT,
                    negative FLOAT,                    
                    neutral FLOAT,
                    CONSTRAINT fk_stock_news
                        FOREIGN KEY(fk_stock_news_id) 
                            REFERENCES stock_news(id)
                            ON DELETE CASCADE
                );
                """
    sql_execution_wrapper(sql_query)


def drop_finbert_summary_sentiment_scores_table() -> None:
    """
    This function deletes the finbert_summary_sentiment_scores table in the database if it exists.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """DROP TABLE IF EXISTS finbert_summary_sentiment_scores;"""
    sql_execution_wrapper(sql_query)


def get_finbert_whole_article_news_ids() -> pd.DataFrame:
    """
    returns a dataframe of the stock_news table

    Parameters:
        None
    Returns:
        pd.DataFrame: returns a dataframe of the stock_news table
    """
    return pd.read_sql_query(
        f"SELECT fk_stock_news_id FROM public.finbert_whole_article_sentiment_scores",
        DB_URL,
    )


def create_finbert_whole_article_sentiment_scores_table() -> None:
    """
    This function creates the finbert_whole_article_sentiment_scores table in the database if it does not exist.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """
                CREATE TABLE IF NOT EXISTS finbert_whole_article_sentiment_scores(
                    id SERIAL PRIMARY KEY,
                    fk_stock_news_id INT UNIQUE,
                    positive FLOAT,
                    negative FLOAT,                    
                    neutral FLOAT,
                    CONSTRAINT fk_stock_news
                        FOREIGN KEY(fk_stock_news_id) 
                            REFERENCES stock_news(id)
                            ON DELETE CASCADE
                );
                """
    sql_execution_wrapper(sql_query)


def drop_finbert_whole_article_sentiment_scores_table() -> None:
    """
    This function deletes the finbert_whole_article_sentiment_scores table in the database if it exists.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """DROP TABLE IF EXISTS finbert_whole_article_sentiment_scores;"""
    sql_execution_wrapper(sql_query)


def create_finbert_tone_summary_sentiment_scores_table() -> None:
    """
    This function creates the finbert_tone_summary_sentiment_scores table in the database if it does not exist.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """
                CREATE TABLE IF NOT EXISTS finbert_tone_summary_sentiment_scores(
                    id SERIAL PRIMARY KEY,
                    fk_stock_news_id INT UNIQUE,
                    positive FLOAT,
                    negative FLOAT,                    
                    neutral FLOAT,
                    CONSTRAINT fk_stock_news
                        FOREIGN KEY(fk_stock_news_id) 
                            REFERENCES stock_news(id)
                            ON DELETE CASCADE
                );
                """
    sql_execution_wrapper(sql_query)


def drop_finbert_tone_summary_sentiment_scores_table() -> None:
    """
    This function deletes the finbert_tone_summary_sentiment_scores table in the database if it exists.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """DROP TABLE IF EXISTS finbert_tone_summary_sentiment_scores;"""
    sql_execution_wrapper(sql_query)


def drop_stock_news_table() -> None:
    """
    This function deletes the stock_news table in the database if it exists.

    Parameters:
        None
    Returns:
        None
    """
    sql_query = """DROP TABLE IF EXISTS stock_news;"""
    sql_execution_wrapper(sql_query)


def insert_into_stock_news_table(articles: List[Dict], ticker: str) -> None:
    """ """
    conn = connect_with_database()
    cur = conn.cursor()

    values = [(art["created"], art["title"], art["url"], ticker) for art in articles]

    cur.executemany(
        """INSERT INTO public.stock_news(date, title, url, ticker) 
           VALUES (%s,%s,%s,%s)""",
        values,
    )

    conn.commit()
    cur.close()
    conn.close()


def update_stock_news_table(id: int, article: str) -> None:
    """ """
    conn = connect_with_database()
    cur = conn.cursor()

    values = (article, id)

    cur.execute(
        """ UPDATE public.stock_news
            SET article = %s
            WHERE id = %s""",
        values,
    )

    conn.commit()
    cur.close()
    conn.close()


def delete_from_stock_news_table(ticker: str) -> None:
    """ """
    conn = connect_with_database()
    cur = conn.cursor()

    values = (ticker,)

    cur.execute(
        """ DELETE FROM public.stock_news           
            WHERE ticker = %s""",
        values,
    )

    conn.commit()
    cur.close()
    conn.close()


def get_stock_news_from_db(ticker: str) -> pd.DataFrame:
    """
    returns a dataframe of the stock_news table

    Parameters:
        None
    Returns:
        pd.DataFrame: returns a dataframe of the stock_news table
    """
    return pd.read_sql_query(
        f"SELECT * FROM public.stock_news WHERE ticker='{ticker}'", DB_URL
    )


def get_stock_news_with_finbert_scores_from_db(ticker: str) -> pd.DataFrame:
    """
    returns a dataframe of the stock_news table with finbert scores on article summaries

    Parameters:
        None
    Returns:
        pd.DataFrame: returns a dataframe of the stock_news table
    """
    return pd.read_sql_query(
        f"""SELECT sn.id, sn.ticker, sn.date, sn.title, sns.summary, fsss.positive, fsss.negative, fsss.neutral
            FROM public.stock_news sn
            JOIN public.stock_news_summaries sns ON sn.id = sns.fk_stock_news_id
            JOIN public.finbert_summary_sentiment_scores fsss ON sn.id = fsss.fk_stock_news_id
            WHERE ticker='{ticker}'""",
        DB_URL,
    )


def get_stock_news_with_finbert_tone_scores_from_db(ticker: str) -> pd.DataFrame:
    """
    returns a dataframe of the stock_news table with finbert-tone scores on article summaries

    Parameters:
        None
    Returns:
        pd.DataFrame: returns a dataframe of the stock_news table
    """
    return pd.read_sql_query(
        f"""SELECT sn.id, sn.ticker, sn.date, sn.title, sns.summary, ftsss.positive, ftsss.negative, ftsss.neutral
            FROM public.stock_news sn
            JOIN public.stock_news_summaries sns ON sn.id = sns.fk_stock_news_id
            JOIN public.finbert_tone_summary_sentiment_scores ftsss ON sn.id = ftsss.fk_stock_news_id
            WHERE ticker='{ticker}'""",
        DB_URL,
    )


def get_news_summaries_from_db(ticker: str) -> pd.DataFrame:
    """
    returns a dataframe of the stock news summaries

    Parameters:
        None
    Returns:
        pd.DataFrame: returns a dataframe of the stock_news table
    """
    return pd.read_sql_query(
        f"""SELECT sn.id, sn.ticker, sn.date, sn.title, sns.summary 
            FROM public.stock_news sn
            JOIN public.stock_news_summaries sns ON sn.id = sns.fk_stock_news_id
            WHERE ticker='{ticker}'
            """,
        DB_URL,
    )
