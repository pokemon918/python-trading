from datetime import datetime 
import time

from database_reference import write_heartbeat
import settings 
       
if __name__ == "__main__":

    while True:
        now = datetime.now()
        write_heartbeat('application1', now, 
                        settings.host, settings.strategies_database, settings.user, settings.password)
        print(f'Heartbeat: application1 = {now}')
        time.sleep(60) 
