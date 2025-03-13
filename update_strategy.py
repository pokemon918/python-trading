import json

from database_strategies import update_strategy_json, get_strategies_by_market
from constants import enum_decoder, enum_encoder
import settings              

if __name__ == "__main__":

    override_strategies = None
    market = 'GC'

    strategies = get_strategies_by_market(market, settings.host, settings.strategies_database, settings.user, settings.password)
    
    for _, row in strategies.iterrows():        

        strategy_id = row['strategy_id']
        strategy_json = row['strategy_json']
        strategy = json.loads(strategy_json, object_hook=enum_decoder)

        if override_strategies is not None:
            if strategy_id not in override_strategies:
                continue
                    
        if 'indicator_reset' not in strategy:
            print(f'Updating {strategy_id}')
            strategy['indicator_reset'] = 0
            parameters_json = json.dumps(strategy, indent=4, cls=enum_encoder)
            update_strategy_json(strategy_id, parameters_json, settings.host, settings.strategies_database, settings.user, settings.password)

    print(f'Finished')
