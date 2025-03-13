import pandas as pd
import time
import numpy as np

import psycopg2
from psycopg2.extras import execute_values

def get_portfolio(host, database, user, password, portfolio_name, optimisation_date):
    
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = """
        SELECT strategies.strategy_id, portfolios.weighting, strategies.market, strategies.json from "portfolios"
        INNER JOIN strategies ON
        strategies.strategy_id = portfolios.strategy_id
        WHERE portfolio_name = %s
        AND portfolios.optimisation_date = %s
    """

    cursor.execute(query, (portfolio_name,optimisation_date))

    strategies = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(strategies, columns=["strategy_id", "weighting", "market", "json"])
    return df

def insert_live_order(account, broker_id, parent_id, order_status, order_type, direction, market, symbol, strategy_id, 
                      submitted_datetime, requested_price, requested_size, timed_exit,
                      host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )
        
        query = """
            INSERT INTO "live_orders" ("account", "broker_id", "parent_id", "order_status", "order_type", "direction", "market", 
            "symbol", "strategy_id", "submitted_datetime", "requested_price", "requested_size", "timed_exit")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        cur = conn.cursor()

        cur.execute(query, (account, broker_id, parent_id, order_status, order_type, direction, market, symbol, strategy_id, 
                    submitted_datetime, requested_price, requested_size, timed_exit))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def update_live_order_status(account, broker_id, status, host, database, user, password):
    query = """
        UPDATE "live_orders"
        SET "order_status" = %s
        WHERE "account" = %s and "broker_id" = '%s';
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (status, account, broker_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def update_live_order_fill(account, broker_id, average_fill_price, filled_size, filled_datetime, host, database, user, password):
    query = """
        UPDATE "live_orders"
        SET "average_fill_price" = %s, "filled_size" = %s, "filled_datetime" = %s
        WHERE "account" = %s and "broker_id" = '%s';
        """    

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (average_fill_price, filled_size, filled_datetime, account, broker_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def update_live_order_return(account, broker_id, returns, host, database, user, password):
    query = """
        UPDATE "live_orders"
        SET "return" = %s
        WHERE "account" = %s and "broker_id" = '%s';
        """    

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (returns, account, broker_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()