import pandas as pd

import psycopg2
from psycopg2.extras import execute_values

def get_unique_strategies_in_poc(portfolio_name, market, host, database, user, password):
    
    query = f"""
        SELECT strategies.strategy_id, strategies.optimisation_date, scores.score FROM strategies 
        INNER JOIN scores ON
        scores.strategy_id = strategies.strategy_id AND scores.optimisation_date = strategies.optimisation_date
        WHERE strategies.strategy_id IN
        (
        SELECT DISTINCT(portfolios.strategy_id) FROM portfolios WHERE portfolio_name = %s
        )
        AND market = %s
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (portfolio_name, market))

        results = cur.fetchall()

        cur.close()
        conn.close()

        # Create a DataFrame with results
        df = pd.DataFrame(results, columns=["strategy_id", "optimisation_date", "score"])

        df['optimisation_date'] = pd.to_datetime(df['optimisation_date'])

        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None

def get_strategies_in_poc(portfolio_name, ranking_score, host, database, user, password):
    
    query = f"""
    SELECT strategies.market, portfolios.strategy_id, portfolios.optimisation_date, scores.{ranking_score}, scores.forward_week_trade_count FROM portfolios 
    INNER JOIN scores ON
    scores.strategy_id = portfolios.strategy_id AND scores.optimisation_date = portfolios.optimisation_date
    INNER JOIN strategies ON
    scores.strategy_id = strategies.strategy_id
    WHERE portfolio_name = %s
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (portfolio_name, ))

        results = cur.fetchall()

        cur.close()
        conn.close()

        # Create a DataFrame with results
        df = pd.DataFrame(results, columns=["market", "strategy_id", "optimisation_date", "score", "forward_week_trade_count"])

        df['optimisation_date'] = pd.to_datetime(df['optimisation_date'])

        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None
    
def get_strategies_above_requirements(markets, use_prediction, requirement_prediction, tawal_requirements, requirements, limits, 
                                      indicator_count, model_name, creation_date_start, creation_date_end,
                                      ranking_score, start_date, end_date,
                                      host, database, user, password):
    
    all_market_strategies_above_requirements = []

    for market in markets:
        market_strategies_above_requirements = get_strategies_above_requirements_for_market(market, use_prediction, requirement_prediction, 
                                      tawal_requirements, requirements[market], limits[market], 
                                      indicator_count, model_name, creation_date_start, creation_date_end,
                                      ranking_score, start_date, end_date,
                                      host, database, user, password)
        if market_strategies_above_requirements is not None and len(market_strategies_above_requirements) > 0:
            all_market_strategies_above_requirements.append(market_strategies_above_requirements)

    combined_strategies_above_requirements = pd.concat(all_market_strategies_above_requirements, ignore_index=True)
    return combined_strategies_above_requirements

def get_strategies_above_requirements_for_market(market, use_prediction, requirement_prediction, tawal_requirements, requirements, limits, 
                                      indicator_count, model_name, creation_date_start, creation_date_end,
                                      ranking_score, start_date, end_date,
                                      host, database, user, password,
                                      only_strategy_optimisation_date = False):
    
    (with_tawal_requirements_SQL, requirements_SQL) = get_build_requirements(
                        True, use_prediction, requirement_prediction, tawal_requirements, requirements, limits,
                        indicator_count, creation_date_start, creation_date_end)
    
    only_strategy_optimisation_date_SQL = ''
    if only_strategy_optimisation_date:
        only_strategy_optimisation_date_SQL = 'and strategies.optimisation_date = scores.optimisation_date'

    query = f"""
        {with_tawal_requirements_SQL}
        SELECT scores.strategy_id, strategies.market, scores.optimisation_date, strategies.json, session_lookup.session_name, predictions.prediction, scores.{ranking_score}            
        FROM scores
        INNER JOIN strategies 
            ON strategies.strategy_id = scores.strategy_id {only_strategy_optimisation_date_SQL}
        INNER JOIN tawal_requirements 
            ON strategies.market = tawal_requirements.market
        LEFT JOIN predictions on
		    predictions.optimisation_date = scores.optimisation_date AND predictions.strategy_id = scores.strategy_id AND predictions.model_name = '{model_name}'
        INNER JOIN session_lookup ON
            session_lookup.session_value = CAST((json::json)->>'session' AS INTEGER)
        WHERE 
            strategies.market = '{market}'
            {requirements_SQL}            
            AND scores.optimisation_date >= %s
            AND scores.optimisation_date <= %s;
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (start_date, end_date))

        results = cur.fetchall()

        cur.close()
        conn.close()

        # Create a DataFrame with results
        df = pd.DataFrame(results, columns=["strategy_id", "market", "optimisation_date", "json", "session", "prediction", "score"])

        if use_prediction:
            df['score'] = df['prediction']

        df = df.drop(columns=["prediction"])

        # Ensure that the scores are sorted decensding
        df.sort_values(by='score', ascending=False, inplace=True)
        
        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None
        
def get_build_requirements(use_requirements, use_prediction, requirement_prediction, tawal_requirements, requirements, limits,
                           indicator_count, creation_date_start, creation_date_end):

    tawal_requirement_SQL = ", ".join([f"('{market}', {requirement})" for market, requirement in tawal_requirements.items()])
    with_tawal_requirements_SQL = f"""
        with tawal_requirements (market,requirement) as (Values 
            {tawal_requirement_SQL}
            ),    
        session_lookup (session_value, session_name) as (Values 
                (0, 'All'),
                (1, 'Asia'),
                (2, 'London'),
                (3, 'US')),
        indicator_reset_lookup (indicator_reset_value, indicator_reset_name) as (Values
                (0, 'Weekly'),
                (1, 'Daily'))    
        """
    
    requirements_SQL = ''    
    if use_requirements:
        requirements_clause = ' AND '.join(f"scores.{key} >= {value}" for key, value in requirements.items() if key != 'indicator_reset')
        limits_clause = ' AND '.join(f"scores.{key} <= {value}" for key, value in limits.items() if key != 'indicator_reset')

        if requirements_clause and limits_clause:
            requirements_SQL = f'AND {requirements_clause} AND {limits_clause}'
        elif requirements_clause:
            requirements_SQL = f'AND {requirements_clause}'
        elif limits_clause:
            requirements_SQL = f'AND {limits_clause}'

        requirements_SQL = f'{requirements_SQL} AND scores.tawal0 >= tawal_requirements.requirement'
        requirements_SQL = f'{requirements_SQL} AND scores.indicator_count <= {indicator_count}'

        if use_prediction:
            requirements_SQL = f'{requirements_SQL} AND predictions.prediction >= {requirement_prediction}'

        if 'indicator_reset' in requirements:            
            indicator_resets_str = ''
            
            if isinstance(requirements['indicator_reset'], list):
                first_reset = True
                
                # Can not use join because that works on strings only
                for reset in requirements["indicator_reset"]:
                    if first_reset:
                        indicator_resets_str = f'{reset.value}'
                        first_reset = False
                    else:
                        indicator_resets_str = f'{indicator_resets_str}, {reset.value}'
            else:
                indicator_resets_str = requirements['indicator_reset'].value

            requirements_SQL = f"{requirements_SQL} AND CAST((json::json)->>'indicator_reset' AS INTEGER) in ({indicator_resets_str})"

    # Always use creation date filters if they are set
    if creation_date_start is not None:
        requirements_SQL = f"{requirements_SQL} AND strategies.creation_datetime >= '{pd.Timestamp(creation_date_start)}'"
    if creation_date_end is not None:
        requirements_SQL = f"{requirements_SQL} AND strategies.creation_datetime <= '{pd.Timestamp(creation_date_end)}'"
    
    return (with_tawal_requirements_SQL, requirements_SQL)

def get_build_scores(use_requirements, markets, tawal_requirements, requirements, limits, portfolio_name, model_name,
                     indicator_count, creation_date_start, creation_date_end, host, database, user, password):
    
    all_market_build_scores = []

    for market in markets:
        market_build_scores = get_build_scores_for_market(use_requirements, market, tawal_requirements, requirements[market], limits[market], 
                                                          portfolio_name, model_name, indicator_count, 
                                                          creation_date_start, creation_date_end, host, database, user, password)
        if market_build_scores is not None and len(market_build_scores) > 0:
            all_market_build_scores.append(market_build_scores)

    combined_scores = pd.concat(all_market_build_scores, ignore_index=True)
    return combined_scores    

def get_build_scores_for_market(use_requirements, market, tawal_requirements, requirements, limits, portfolio_name, model_name,
                     indicator_count, creation_date_start, creation_date_end, host, database, user, password):
    
    (with_tawal_requirements_SQL, requirements_SQL) = get_build_requirements(
                    use_requirements, False, 0.0, tawal_requirements, requirements, limits,
                    indicator_count, creation_date_start, creation_date_end)
    
    query = f"""
        {with_tawal_requirements_SQL}
        SELECT strategies.strategy_id, strategies.run_id, runs.version, md5(config),
        strategies.creation_datetime, strategies.optimisation_date, strategies.market, 
        CAST((json::json)->>'monday' AS INTEGER) as monday,
        CAST((json::json)->>'tuesday' AS INTEGER) as tuesday,
        CAST((json::json)->>'wednesday' AS INTEGER) as wednesday,
        CAST((json::json)->>'thursday' AS INTEGER) as thursday,
        CAST((json::json)->>'friday' AS INTEGER) as friday,
        session_lookup.session_name,
        indicator_reset_lookup.indicator_reset_name,
        scores.max_trade_length,
        scores.optimisation_date, 
        runs.improver_id,
        runs.improver_inital_score,
        runs.improver_final_score,
        COALESCE(predictions.prediction, 0) AS prediction,
        COALESCE(portfolios.weighting, 0) AS weighting,
        scores.forward_week_returns,
        scores.forward_week_trade_count,
        scores.forward_week_trade_win_count,
        scores.forward_week_average_win,
        scores.forward_week_average_loss,
        scores.weeks_after_optimisation,
        scores.score,
        scores.edge_better_than_random0,
        scores.edge_trade_count0,
        scores.edge_trade_count_over_independent_variables_mst0_52,
        scores.trade_count_over_independent_variables0,
        scores.trade_count_over_independent_variables_over_weeks0,
        scores.mean_reverting_flag,
        scores.cost_percentage0,
        scores.indicator_count,
        scores.profit_target_over_stoploss,
        scores.tradable_weeks_rate0,
        scores.profit_target_percentage0,
        scores.stop_loss_percentage0,
        scores.profit_target_stop_loss_percentage0,
        scores.aar0,
        scores.aart0,
        scores.min_sharpes0_4,
        scores.min_sharpes0_8,
        scores.min_sharpes0_13,
        scores.min_sharpes0_104,
        scores.mst0_52_aart0,
        scores.mst0_aart0_edge_better_than_random0,
        scores.mst0_52_trade_to_signal_percentage,
        scores.min_sharpes0,
        scores.min_sharpes520,
        scores.min_sharpes208,
        scores.min_sharpes156,
        scores.min_sharpes104,
        scores.min_sharpes52,
        scores.min_sharpes26,
        scores.min_sharpes13,
        scores.min_sharpes8,
        scores.min_sharpes4,
        scores.nmr0,
        scores.nmr520,
        scores.nmr208,
        scores.nmr156,
        scores.nmr104,
        scores.nmr52,
        scores.tawal0,
        scores.tawal520,
        scores.tawal208,
        scores.tawal156,
        scores.tawal104,
        scores.tawal52,
        scores.tawal26,
        scores.tawal13,
        scores.tawal8,
        scores.tawal4,
        scores.trade_win_rate0,
        scores.trade_win_rate520,
        scores.trade_win_rate208,
        scores.trade_win_rate156,
        scores.trade_win_rate104,
        scores.trade_win_rate52,
        scores.trade_win_rate26,
        scores.trade_win_rate13,
        scores.trade_win_rate8,
        scores.trade_win_rate4,
        scores.trade_win_over_loss0,
        scores.trade_win_over_loss520,
        scores.trade_win_over_loss208,
        scores.trade_win_over_loss156,
        scores.trade_win_over_loss104,
        scores.trade_win_over_loss52,
        scores.trade_win_over_loss26,
        scores.trade_win_over_loss13,
        scores.trade_win_over_loss8,
        scores.trade_win_over_loss4,
        scores.trade_average0,
        scores.trade_average520,
        scores.trade_average208,
        scores.trade_average156,
        scores.trade_average104,
        scores.trade_average52,
        scores.trade_average26,
        scores.trade_average13,
        scores.trade_average8,
        scores.trade_average4,
        scores.trade_count0,
        scores.trade_count520,
        scores.trade_count208,
        scores.trade_count156,
        scores.trade_count104,
        scores.trade_count52,
        scores.trade_count26,
        scores.trade_count13,
        scores.trade_count8,
        scores.trade_count4,
        scores.highest_trade_count_week0,
        scores.highest_trade_count_day0,
        scores.trade_to_signal_percentage0,
        scores.average_correlation_signal0,
        scores.trade_mst0,
        scores.trade_mst520,
        scores.trade_mst208,
        scores.trade_mst156,
        scores.trade_mst104,
        scores.trade_mst52,
        scores.trade_mst26,
        scores.trade_mst13,
        scores.trade_mst8,
        scores.trade_mst4

        FROM scores
        INNER JOIN strategies ON
        strategies.strategy_id = scores.strategy_id 
        -- AND scores.optimisation_date = strategies.optimisation_date
        inner join tawal_requirements on
        strategies.market = tawal_requirements.market
        INNER JOIN runs ON
        runs.strategy_id = strategies.strategy_id
        INNER JOIN session_lookup ON
        session_lookup.session_value = CAST((json::json)->>'session' AS INTEGER)
        INNER JOIN indicator_reset_lookup ON
        indicator_reset_lookup.indicator_reset_value = COALESCE(CAST((json::json)->>'indicator_reset' AS INTEGER), 0)
        LEFT JOIN predictions on
		predictions.optimisation_date = scores.optimisation_date AND predictions.strategy_id = scores.strategy_id and predictions.model_name = '{model_name}'
        left JOIN portfolios on
        portfolios.portfolio_name = '{portfolio_name}' AND portfolios.optimisation_date = scores.optimisation_date AND portfolios.strategy_id = strategies.strategy_id
        WHERE strategies.market = '{market}'
        {requirements_SQL}
        ORDER BY strategies.creation_datetime desc
    """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query)

        results = cur.fetchall()

        cur.close()
        conn.close()
        
        columns = [
            "strategy_id", "run_id", "version", "config_hash", "creation_datetime", "strategy_optimisation_date", "market", 
            "monday", "tuesday", "wednesday", "thursday", "friday",
            "session", "indicator_reset", "max_trade_length", "optimisation_date", 
            "improver_id", "improver_inital_score", "improver_final_score",
            "prediction", "weighting", "forward_week_returns", "forward_week_trade_count",         
            "forward_week_trade_win_count", "forward_week_average_win", "forward_week_average_loss", "weeks_after_optimisation", 
            "score", "edge_better_than_random0", "edge_trade_count0", "edge_trade_count_over_independent_variables_mst0_52", 
            "trade_count_over_independent_variables0", "trade_count_over_independent_variables_over_weeks0", "mean_reverting_flag", "cost_percentage0",
            "indicator_count", "profit_target_over_stoploss", "tradable_weeks_rate0", 
            "profit_target_percentage0", "stop_loss_percentage0", "profit_target_stop_loss_percentage0",
            "aar0", "aart0", "min_sharpes0_4", "min_sharpes0_8", "min_sharpes0_13", "min_sharpes0_104", 
            "mst0_52_aart0", "mst0_aart0_edge_better_than_random0", "mst0_52_trade_to_signal_percentage",
            "min_sharpes0", "min_sharpes520", "min_sharpes208", "min_sharpes156", 
            "min_sharpes104", "min_sharpes52", "min_sharpes26", "min_sharpes13", "min_sharpes8", "min_sharpes4",
            "nmr0", "nmr520", "nmr208", "nmr156", "nmr104", "nmr52",
            "tawal0", "tawal520", "tawal208", "tawal156", "tawal104", "tawal52", "tawal26", "tawal13", "tawal8", 
            "tawal4", "trade_win_rate0", "trade_win_rate520", "trade_win_rate208", "trade_win_rate156", 
            "trade_win_rate104", "trade_win_rate52", "trade_win_rate26", "trade_win_rate13", "trade_win_rate8", 
            "trade_win_rate4", "trade_win_over_loss0", "trade_win_over_loss520", "trade_win_over_loss208", 
            "trade_win_over_loss156", "trade_win_over_loss104", "trade_win_over_loss52", "trade_win_over_loss26", 
            "trade_win_over_loss13", "trade_win_over_loss8", "trade_win_over_loss4", "trade_average0", 
            "trade_average520", "trade_average208", "trade_average156", "trade_average104", "trade_average52", 
            "trade_average26", "trade_average13", "trade_average8", "trade_average4", "trade_count0", 
            "trade_count520", "trade_count208", "trade_count156", "trade_count104", "trade_count52", 
            "trade_count26", "trade_count13", "trade_count8", "trade_count4", "highest_trade_count_week0", "highest_trade_count_day0", 
            "trade_to_signal_percentage0", "average_correlation_signal0", 
            "trade_mst0", "trade_mst520", "trade_mst208", "trade_mst156", "trade_mst104", "trade_mst52", "trade_mst26", "trade_mst13", "trade_mst8", "trade_mst4"]
        df = pd.DataFrame(results, columns=columns)        
        df['creation_datetime'] = pd.to_datetime(df['creation_datetime'])
        df['strategy_optimisation_date'] = pd.to_datetime(df['strategy_optimisation_date'])
        df['optimisation_date'] = pd.to_datetime(df['optimisation_date'])

        return df

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        return None
