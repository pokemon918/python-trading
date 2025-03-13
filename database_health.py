import pandas as pd
import time
import numpy as np

import psycopg2
from psycopg2.extras import execute_values

def get_latest_critical_activity(host, database, user, password, last_id):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
    SELECT activity_id, component, datetime, severity, message from activity
    where 
    activity_id > %s
    and (severity = 1 or send_alert = True)
    and alert_sent_datetime is null
    order by activity_id
    """

    cursor.execute(query, (last_id,))

    data = cursor.fetchall()

    cursor.close()
    conn.close()    

    df = pd.DataFrame(data, columns=["activity_id", "component", "datetime", "severity", "message"])

    df['datetime'] = pd.to_datetime(df['datetime'])

    return df

def get_heartbeats(host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
    SELECT component, datetime from heartbeats    
    """

    cursor.execute(query)

    data = cursor.fetchall()

    cursor.close()
    conn.close()    

    df = pd.DataFrame(data, columns=["component", "datetime"])

    df['datetime'] = pd.to_datetime(df['datetime'])

    return df

def write_heartbeat(component, datetime, host, database, user, password):
    query = """
        INSERT INTO "heartbeats" ("component", "datetime")
        VALUES (%s, %s)
        ON CONFLICT (component)
        DO UPDATE SET "datetime" = EXCLUDED."datetime";
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (component, datetime))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def write_activity(component, datetime, severity, message, send_alert, save_activity, host, database, user, password):

    if not save_activity:
        print(f'{datetime} - {severity.name}: {component} = {message}')
        return

    query = """
        INSERT INTO "activity" 
            ("component", "datetime", "severity", "message", "send_alert")
            VALUES (%s, %s, %s, %s, %s);
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (component, datetime, severity.value, message, send_alert))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def write_activity_integrity_batch(activity_dict, datetime, market, component, severity, save_activity,
                                   host, database, user, password):
    
    if not save_activity:
        for datetime, failed_names in activity_dict.items():
            print(f'Activity: {datetime} - {severity.name}: {component} = {failed_names}')
        return

    query = """
        INSERT INTO "activity" 
            ("component", "datetime", "severity", "message")
            VALUES (%s, %s, %s, %s);
    """

    # Build the list of rows to insert.
    rows = []
    for failed_datetime, failed_names in activity_dict.items():
        failed_names_string = ", ".join(failed_names)
        rows.append((component, datetime, severity.value, f'Data Integrity Failure for {market} {failed_datetime}: {failed_names_string}'))

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )
        cur = conn.cursor()
        # Use executemany to batch process all rows
        cur.executemany(query, rows)
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def get_acceptable_gaps(market, host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT start_datetime, end_datetime, reoccur_day_of_week, reoccur_start_time, reoccur_end_time, description from acceptable_gaps where market = %s
        """

    cursor.execute(query, (market,))

    data = cursor.fetchall()    

    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=['start_datetime', 'end_datetime', 'reoccur_day_of_week', 'reoccur_start_time', 'reoccur_end_time', 'description'])
    df['start_datetime'] = pd.to_datetime(df['start_datetime'])
    df['end_datetime'] = pd.to_datetime(df['end_datetime'])

    return df
