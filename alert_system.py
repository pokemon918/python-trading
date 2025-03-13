import sys
import smtplib
import atexit
import time
import threading
from datetime import datetime
from email.mime.text import MIMEText

import psycopg2
import select

from constants import Severity
from database_health import get_latest_critical_activity, write_activity, get_heartbeats
import settings

refresh_activity_seconds = 5
refresh_heartbeats_seconds = 60

heartbeat_requirement_default = 60
heartbeat_requirements = {
    'TestComponent1': 60,
    'TestComponent2': 60,
}

def send_email_alert(component, datetime, message):
    
    # Uses Direct Send of our outlook server. It can only send emails on-premises.
    # https://learn.microsoft.com/en-us/exchange/mail-flow-best-practices/how-to-set-up-a-multifunction-device-or-application-to-send-email-using-microsoft-365-or-office-365
    smtp_server = 'savernakecapital-com.mail.protection.outlook.com'
    smtp_port = 25
    from_addr = 'systemalerts@savernaketechnology.com'
    to_addr = 'mark@savernakecapital.com'

    subject = f"Critical Alert: {component} - {message}"
    body = (
        f"Timestamp: {datetime}\n"
        f"Component: {component}\n"
        f"Message: {message}\n"
    )

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        print(f"Email alert sent for critical {message}")
    except Exception as e:
        print("Error sending email:", e)

        write_activity(component, datetime.now(), Severity.Critical, "Error while sending alert email", False, save_activity,
                       settings.host, settings.strategies_database, settings.user, settings.password)


def application_exit():
    write_activity(component, datetime.now(), Severity.Info, "Stopped application", False, save_activity,
                   settings.host, settings.strategies_database, settings.user, settings.password)

def timer_heartbeat_check():
    existing_heartbeat_alerts = {}

    while True:
        datetime_now = datetime.now()

        component_heartbeats = get_heartbeats(settings.host, settings.strategies_database, settings.user,
                                              settings.password)
        for index, row in component_heartbeats.iterrows():
            heartbeat_component = row['component']
            heartbeat_datetime = row['datetime']

            time_diff = datetime_now - heartbeat_datetime
            seconds_diff = time_diff.total_seconds()

            heartbeat_requirement = heartbeat_requirement_default
            if component in heartbeat_requirements:
                heartbeat_requirement = heartbeat_requirements[component]

            if seconds_diff >= heartbeat_requirement and heartbeat_component not in existing_heartbeat_alerts:
                existing_heartbeat_alerts[heartbeat_component] = datetime_now
                write_activity(heartbeat_component, datetime.now(), Severity.Critical, "No heartbeats", False, save_activity,
                               settings.host, settings.strategies_database, settings.user, settings.password)

            if seconds_diff < heartbeat_requirement and heartbeat_component in existing_heartbeat_alerts:
                del existing_heartbeat_alerts[heartbeat_component]
                write_activity(heartbeat_component, datetime.now(), Severity.Info, "Heartbeats have resumed", True, save_activity,
                               settings.host, settings.strategies_database, settings.user, settings.password)

        time.sleep(refresh_heartbeats_seconds)

def listen_activity_update(host, database, user, password):
    try:

        last_processed_id = 0

        conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password)        

        # Set autocommit mode to get notifications immediately.
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("LISTEN activity_update;")
        
        while True:
            if select.select([conn], [], [], 5) == ([], [], []):
                continue
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    # print("Notification received:", notify.payload)

                    recent_critical_activities = get_latest_critical_activity(settings.host, settings.strategies_database,
                                                                              settings.user, settings.password, last_processed_id)

                    for index, row in recent_critical_activities.iterrows():
                        send_email_alert(row['component'], row['datetime'], row['message'])
                        last_processed_id = index
                    
    except Exception as e:
        send_email_alert('AlertSystem', datetime.now(), "Exception while listening for critical errors")
        print(f"{datetime.now()}: Exception while listening for critical errors:", e)
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':

    save_activity = False
    alert_missed_heartbeat_seconds = 5
    component = 'AlertSystem'

    atexit.register(application_exit)

    # send_email_alert('TestComponent', datetime.now(), "Test Message")

    write_activity(component, datetime.now(), Severity.Info, "Started application", False, save_activity,
                   settings.host, settings.strategies_database, settings.user, settings.password)

    thread_alert = threading.Thread(target=listen_activity_update, 
                                    args=(settings.host, settings.strategies_database, settings.user, settings.password), 
                                    daemon=True)
    thread_heartbeat = threading.Thread(target=timer_heartbeat_check, daemon=True)

    thread_alert.start()
    thread_heartbeat.start()

    stop_event = threading.Event()
    # This will block indefinitely to keep the main thread alive
    stop_event.wait()
