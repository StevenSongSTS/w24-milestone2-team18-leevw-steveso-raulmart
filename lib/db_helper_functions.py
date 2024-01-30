from typing import Dict, List

import psycopg2

from lib.constants import DB_URL


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
                );
                """
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
