import datetime
import datetime as dt
import os
from math import floor

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import find_dotenv, load_dotenv
from stravalib import Client

load_dotenv(find_dotenv(".env"))

import logging

LOGGER = logging.getLogger("airflow.task")


def sendtext(api_key: str, message: str, phone: str, verbose: bool = True):
    # print(f'{api_key=}')
    uri = "https://textbelt.com/text"
    resp = requests.post(uri, {"phone": phone, "message": message, "key": api_key})
    if verbose:
        LOGGER.info(resp.json())


def has_run_today(execution_date):
    phone_number = os.environ["PHONE_NUMBER"]
    LOGGER.info(f"{phone_number=}")
    LOGGER.info(f"{execution_date=}")
    start = datetime.date(2016, 2, 26)
    completed = (datetime.date.today() - start).days
    today = completed + 1

    # connect to strava api client
    # now = datetime.datetime.now()

    # the default time zone to set to is the system local timezone
    # now = execution_date.astimezone()
    # airflow uses pendulum, so the above would work, but explicit over default:
    now = execution_date.astimezone(pendulum.timezone("EST"))
    LOGGER.info(f"{now=}")
    client = Client()

    refresh_response = client.refresh_access_token(
        client_id=os.environ["STRAVA_CLIENT_ID"],
        client_secret=os.environ["STRAVA_CLIENT_SECRET"],
        refresh_token=os.environ["STRAVA_REFRESH_TOKEN"],
    )

    access_token = refresh_response["access_token"]
    # print(f'{access_token=}')
    refresh_token = refresh_response["refresh_token"]
    expires_at = refresh_response["expires_at"]

    client.access_token = access_token
    client.refresh_token = refresh_token
    client.token_expires_at = expires_at
    beginning_today = datetime.datetime(now.year, now.month, now.day)
    beginning_last_week = beginning_today - datetime.timedelta(days=6)
    # blw_iso = beginning_last_week.isoformat()
    # read most recent activity
    last_week_activities = [
        activity for activity in client.get_activities(after=beginning_last_week)
    ]
    runs = [
        activity
        for activity in last_week_activities
        if ((activity.type == "Run") or (activity.type == "VirtualRun"))
    ]
    run_today = any([run.start_date_local > beginning_today for run in runs])
    for r in runs:
        LOGGER.info(
            f"{r.name} ({r.type}) {r.distance} {r.moving_time} {r.distance.num/1609:.2f} {r.moving_time.seconds/60:.1f}"
        )
    total_distance = sum([x.distance.num for x in runs]) / 1609  # miles
    total_time = sum([x.moving_time.seconds for x in runs]) / 60  # minutes
    total_kudos = sum([x.kudos_count for x in runs])
    average_pace_float = total_time / total_distance
    LOGGER.info(f"{average_pace_float=}")
    average_pace_minmi = f"{floor(average_pace_float):.0f}:{(average_pace_float-floor(average_pace_float))*60:02.0f}"
    LOGGER.info(f"{average_pace_minmi=}")
    total_time_hours_minutes = (
        f"{floor(total_time/60):.0f}:{(total_time-floor(total_time/60)*60):02.0f}"
    )
    LOGGER.info(f"{total_time_hours_minutes=}")

    runs_cal_wk = [
        activity
        for activity in last_week_activities
        if ((activity.type == "Run") or (activity.type == "VirtualRun"))
        and activity.start_date_local.weekday() <= now.weekday()
    ]
    total_distance_cal_wk = sum([x.distance.num for x in runs_cal_wk]) / 1609  # miles
    total_time_cal_wk = (
        sum([x.moving_time.seconds for x in runs_cal_wk]) / 60
    )  # minutes
    total_kudos_cal_wk = sum([x.kudos_count for x in runs_cal_wk])
    if total_distance_cal_wk == 0:
        average_pace_float_cal_wk = 0
    else:
        average_pace_float_cal_wk = total_time_cal_wk / total_distance_cal_wk
    average_pace_minmi_cal_wk = f"{floor(average_pace_float_cal_wk):.0f}:{(average_pace_float_cal_wk-floor(average_pace_float_cal_wk))*60:02.0f}"
    total_time_hours_minutes_cal_wk = f"{floor(total_time_cal_wk/60):.0f}:{(total_time_cal_wk-floor(total_time_cal_wk/60)*60):02.0f}"

    # if none today, and it's past 11pm, sent a text!
    go_run_alert = f"GO RUN. This is day {today}."
    LOGGER.info(f"{run_today=}")
    if not run_today:
        LOGGER.info(f"{go_run_alert=}")
        if now.hour > 22:
            LOGGER.info("Alerting to GO RUN")
            sendtext(
                os.environ.get("TEXTBELT_API_KEY", "textbelt"),
                go_run_alert,
                phone_number,
            )
    run_recent = any(
        [
            (run.start_date_local + run.elapsed_time)
            > (datetime.datetime.now() - datetime.timedelta(minutes=10))
            for run in runs
        ]
    )
    run_recent_msg = f"Great job on day {today}, 7-day running {total_distance:.1f}mi in {total_time_hours_minutes} at {average_pace_minmi}min/mi, cal wk {total_distance_cal_wk:.1f}mi in {total_time_hours_minutes_cal_wk} at {average_pace_minmi_cal_wk}min/mi."
    LOGGER.info(f"{run_recent=}")
    LOGGER.info(f"{run_recent_msg=}")
    if run_recent:
        LOGGER.info(f"{run_recent_msg=}")
        sendtext(
            os.environ.get("TEXTBELT_API_KEY", "textbelt"), run_recent_msg, phone_number
        )


default_args = {
    "owner": "me",
    "start_date": dt.datetime(2017, 6, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


with DAG(
    "strava_text",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
) as dag:

    #     t1 = BashOperator(
    #         task_id='print_date',
    #         bash_command='date',
    #         dag=dag,
    #     )

    has_run_today_job = PythonOperator(
        task_id="has_run_today",
        python_callable=has_run_today,
        dag=dag,
    )


# [t1, has_run_today_job]
has_run_today_job
