import pandas as pd
import numpy as np

import psycopg2
from psycopg2.extras import execute_values


def insert_strategy(run_id, creation_datetime, optimisation_date, market, json_data, host, database, user, password):
    query = """
        INSERT INTO "strategies" ("run_id", "creation_datetime", "optimisation_date", "market", "json")
        VALUES (%s, %s, %s, %s, %s)
        RETURNING "strategy_id";
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (run_id, creation_datetime, optimisation_date, market, json_data))
        strategy_id = cur.fetchone()[0]

        conn.commit()

        cur.close()
        conn.close()

        return strategy_id

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def update_strategy_json(strategy_id, json_data, host, database, user, password):
    query = """
        UPDATE strategies set json = %s where strategy_id = %s;
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (json_data, strategy_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def insert_run(config_filename, config, version, start_datetime, market, optimisation_date, population_size,
               improver_id, fixed_days, fixed_session,
               machine_name, process_id, host, database, user, password):
    query = """
        INSERT INTO "runs" 
            ("config_filename", "config", "version", "start_datetime", "market", "optimisation_date", "population_size", "improver_id", "day", "session", "machine_name", "process_id")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING "run_id";
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (
        config_filename, config, version, start_datetime, market, optimisation_date, population_size, improver_id,
        fixed_days, fixed_session, machine_name, process_id))
        run_id = cur.fetchone()[0]

        conn.commit()

        cur.close()
        conn.close()

        return run_id

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def insert_scores(strategy_id, optimisation_date, score_data, host, database, user, password):
    columns = ', '.join(score_data.keys())
    values = ', '.join(['%s'] * len(score_data))

    query = f"""
        INSERT INTO "scores" ("strategy_id", "optimisation_date", {columns})
        VALUES (%s, %s, {values})
        ON CONFLICT ("strategy_id", "optimisation_date")
        DO UPDATE 
        SET 
            {', '.join([f'"{key}" = EXCLUDED."{key}"' for key in score_data.keys()])};
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (strategy_id, optimisation_date, *score_data.values()))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def replace_portfolio(portfolio_name, market_units, host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        delete_query = """
            DELETE FROM "portfolios" 
            WHERE "portfolio_name" = %s;
        """
        cur.execute(delete_query, (portfolio_name,))

        insert_query = """
            INSERT INTO "portfolios" ("portfolio_name", "optimisation_date", "strategy_id", "weighting")
            VALUES %s;
        """

        insert_data = []
        for market, optimisation_dates in market_units.items():
            for optimisation_date, strategies in optimisation_dates.items():
                for strategy_id, weighting in strategies.items():
                    insert_data.append((portfolio_name, optimisation_date, strategy_id, weighting))

        if insert_data:
            execute_values(cur, insert_query, insert_data)

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def delete_portfolio(portfolio_name, host, database, user, password):
    query = """
        DELETE FROM "portfolios"
        WHERE "portfolio_name" = %s;
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (portfolio_name,))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def get_predictions(model_name, host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT "optimisation_date", "strategy_id", "prediction" FROM predictions where model_name = %s
        """

    cursor.execute(query, (model_name,))

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["optimisation_date", "strategy_id", "prediction"])
    df['optimisation_date'] = pd.to_datetime(df['optimisation_date'])

    return df


def replace_predictions(model_name, predictions, host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        delete_query = """
            DELETE FROM "predictions" 
            WHERE "model_name" = %s;
        """
        cur.execute(delete_query, (model_name,))

        insert_query = """
            INSERT INTO "predictions" ("model_name", "optimisation_date", "strategy_id", "prediction")
            VALUES %s;
        """

        insert_data = []
        for index, row in predictions.iterrows():
            insert_data.append((model_name, row['PortfolioDate'], row['ID'], row['Prediction']))

        if insert_data:
            execute_values(cur, insert_query, insert_data)

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def update_run(run_id, strategy_id, end_datetime, generations, best_score, gens_to_positive, host, database, user, password):
    query = """
        UPDATE "runs"
        SET "strategy_id" = %s, "end_datetime" = %s, "generations" = %s, "best_score" = %s, "gens_to_positive" = %s
        WHERE "run_id" = %s;
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (strategy_id, end_datetime, generations, best_score, gens_to_positive, run_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def update_run_improver(run_id, improver_inital_score, improver_final_score, host, database, user, password):
    query = """
        UPDATE "runs"
        SET "improver_inital_score" = %s, "improver_final_score" = %s
        WHERE "run_id" = %s;
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (improver_inital_score, improver_final_score, run_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def get_strategy(strategy_id, host, database, user, password):
    query = """
        SELECT market, optimisation_date, json
        FROM strategies
        WHERE strategy_id = %s;
        """
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()
        cur.execute(query, (strategy_id,))

        result = cur.fetchone()

        if result is None:
            print(f"No data found for strategy_id {strategy_id}.")
            return None

        market, optimisation_date, parameters_json = result

        cur.close()
        conn.close()

        return (market, optimisation_date, parameters_json)

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None


def get_strategies(market_overide, creation_date_start, creation_date_end, host, database, user, password):
    query = f"""
    SELECT strategies.strategy_id, strategies.optimisation_date, strategies.market, scores.score FROM strategies
    INNER JOIN scores ON
    scores.strategy_id = strategies.strategy_id AND scores.optimisation_date = strategies.optimisation_date
    WHERE strategies.market = %s
    """

    # Always use creation date filters if they are set
    if creation_date_start is not None:
        query = f"{query} AND strategies.creation_datetime >= '{pd.Timestamp(creation_date_start)}'"
    if creation_date_end is not None:
        query = f"{query} AND strategies.creation_datetime <= '{pd.Timestamp(creation_date_end)}'"

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (market_overide,))

        results = cur.fetchall()

        cur.close()
        conn.close()

        # Create a DataFrame with results
        df = pd.DataFrame(results, columns=["strategy_id", "optimisation_date", "market", "score"])

        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None


def get_strategies_by_market(market, host, database, user, password):
    query = f"""
    SELECT strategies.strategy_id, strategies.json FROM strategies
    WHERE strategies.market = %s
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (market,))

        results = cur.fetchall()

        cur.close()
        conn.close()

        # Create a DataFrame with results
        df = pd.DataFrame(results, columns=["strategy_id", "strategy_json"])

        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None


def get_strategy_returns(strategy_id, datetimes_1minute, host, database, user, password, datetime_to_index=None,
                         minute_count=None):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
    SELECT datetime, return
    FROM returns
    WHERE strategy_id = %s
    """

    cursor.execute(query, (strategy_id,))

    data = cursor.fetchall()

    cursor.close()
    conn.close()

    if datetime_to_index is None:
        datetime_to_index = {dt: idx for idx, dt in enumerate(datetimes_1minute)}
        returns = np.zeros_like(datetimes_1minute, dtype=float)
    else:
        returns = np.zeros(minute_count, dtype=float)

    for (db_datetime, db_return) in data:
        if db_datetime in datetime_to_index:
            idx = datetime_to_index[db_datetime]
            returns[idx] = db_return

    return returns


def write_strategy_returns(strategy_id, datetimes_1minute, returns, host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO returns (strategy_id, datetime, return)
        VALUES %s
        ON CONFLICT (strategy_id, datetime) DO UPDATE
            SET return = EXCLUDED.return
    """

    data_to_insert = [(strategy_id, dt, ret) for dt, ret in zip(datetimes_1minute, returns) if ret != 0]
    if data_to_insert:
        execute_values(cursor, insert_query, data_to_insert)

    conn.commit()

    cursor.close()
    conn.close()


def delete_strategy_returns(strategy_id, host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        query = """
            DELETE FROM returns
            WHERE strategy_id = %s;
        """

        cur = conn.cursor()

        cur.execute(query, (strategy_id,))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def get_all_trades(host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
    SELECT strategy_id, direction, entry_datetime, exit_datetime, entry_price, exit_price, return
    FROM trades
    ORDER BY strategy_id, entry_datetime
    """

    cursor.execute(query)

    data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=["strategy_id", "direction", "entry_datetime", "exit_datetime", "entry_price",
                                     "exit_price", "return"])

    df['entry_datetime'] = pd.to_datetime(df['entry_datetime'])
    df['exit_datetime'] = pd.to_datetime(df['exit_datetime'])

    df['entry_price'] = pd.to_numeric(df['entry_price'], errors='coerce')
    df['exit_price'] = pd.to_numeric(df['exit_price'], errors='coerce')
    df['return'] = pd.to_numeric(df['return'], errors='coerce')

    return df


def get_strategy_trades(strategy_id, host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
    SELECT strategy_id, direction, entry_datetime, exit_datetime, entry_price, exit_price, return, entry_price_before_slippage, exit_price_before_slippage
    FROM trades
    WHERE strategy_id = %s
    ORDER BY entry_datetime
    """

    cursor.execute(query, (strategy_id,))

    data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=["strategy_id", "direction", "entry_datetime", "exit_datetime", "entry_price",
                                     "exit_price", "return", "entry_price_before_slippage",
                                     "exit_price_before_slippage"])

    df['entry_datetime'] = pd.to_datetime(df['entry_datetime'])
    df['exit_datetime'] = pd.to_datetime(df['exit_datetime'])

    df['entry_price'] = pd.to_numeric(df['entry_price'], errors='coerce')
    df['exit_price'] = pd.to_numeric(df['exit_price'], errors='coerce')
    df['return'] = pd.to_numeric(df['return'], errors='coerce')
    df['entry_price_before_slippage'] = pd.to_numeric(df['entry_price_before_slippage'], errors='coerce')
    df['exit_price_before_slippage'] = pd.to_numeric(df['exit_price_before_slippage'], errors='coerce')

    return df


def write_strategy_trades(strategy_id, df, host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO trades (strategy_id, direction, entry_datetime, exit_datetime, entry_price, exit_price, return, reason, entry_price_before_slippage, exit_price_before_slippage)
        VALUES %s
        ON CONFLICT (strategy_id, entry_datetime) DO UPDATE
        SET direction = EXCLUDED.direction,
            exit_datetime = EXCLUDED.exit_datetime,
            entry_price = EXCLUDED.entry_price,
            exit_price = EXCLUDED.exit_price,
            return = EXCLUDED.return,
            reason = EXCLUDED.reason,
            entry_price_before_slippage = EXCLUDED.entry_price_before_slippage,
            exit_price_before_slippage = EXCLUDED.exit_price_before_slippage
        """

    data_to_insert = [(strategy_id, row['Direction'], row['Entry DateTime'], row['Exit DateTime'],
                       row['Entry Price'], row['Exit Price'], row['Return'], row['Reason'],
                       row['Entry Price Before Slippage'], row['Exit Price Before Slippage']) for _, row in
                      df.iterrows()]

    execute_values(cursor, insert_query, data_to_insert)

    conn.commit()

    cursor.close()
    conn.close()


def delete_strategy_trades(strategy_id, host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        query = """
            DELETE FROM trades
            WHERE strategy_id = %s;
        """

        cur = conn.cursor()

        cur.execute(query, (strategy_id,))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def insert_config(config_type, datetime, config, host, database, user, password):
    query = """
        INSERT INTO "configs" 
            ("config_type", "datetime", "config")
            VALUES (%s, %s, %s);
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (config_type, datetime, config))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()


def get_latest_config(config_type, host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT config from configs WHERE config_type = %s order by datetime desc limit 1;
        """

    cursor.execute(query, (config_type,))

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result[0]


def get_last_scores(host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT DISTINCT ON (scores.strategy_id) scores.strategy_id, scores.optimisation_date, strategies.market, strategies.optimisation_date, strategies.json, 
        CAST((json::json)->>'indicator_reset' AS INTEGER) as indicator_reset
        FROM scores 
        LEFT JOIN strategies ON scores.strategy_id = strategies.strategy_id
        ORDER BY scores.strategy_id, scores.optimisation_date DESC;
        """

    cursor.execute(query)

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data,
                      columns=["strategy_id", "score_datetime", "market", "strategy_optimisation_date", "strategy_json",
                               "indicator_reset"])
    df['score_datetime'] = pd.to_datetime(df['score_datetime'])
    df['strategy_optimisation_date'] = pd.to_datetime(df['strategy_optimisation_date'])
    df.set_index('strategy_id', inplace=True, drop=False)

    return df


def update_run_oom(process_id, machine_name, datetime, host, database, user, password):
    query = """
        UPDATE "runs"
        SET "oom_error" = true, "end_datetime" = %s
        WHERE "run_id" in 
        (
        SELECT run_id FROM runs WHERE "machine_name" = %s and "process_id" = %s ORDER BY start_datetime DESC LIMIT 1
        )
        and "end_datetime" is null
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (datetime, machine_name, process_id))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()