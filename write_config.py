from datetime import datetime

from database_strategies import insert_config
import settings

if __name__ == "__main__":        

    datetime_now = datetime.now()

    with open('settings.py', 'r') as settings_file:
        config = settings_file.read()

    insert_config(settings.config_type, datetime_now, config, 
                  settings.host, settings.strategies_database,
                  settings.user, settings.password)

    print(f'Written latest config to database at {datetime_now}')
    