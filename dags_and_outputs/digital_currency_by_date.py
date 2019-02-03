import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor

import pandas as pd
import requests
import os
import io
import logging
import time
from datetime import datetime, timedelta, date
import matplotlib

from digital_currency_daily import API_ENDPOINT, API_FUNCTION, API_DATATYPE, get_logger


API_KEY = "PMCQ61RYQ7T7TUFY"
SENSOR_CURRENCY = os.environ['SENSOR_CURRENCY']
SENSOR_DATE = os.environ['SENSOR_DATE']

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2, hour=12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='digital_currency_by_date',
    default_args=args,
    schedule_interval=None,
    catchup=False
)

params = {'function': API_FUNCTION,
          'symbol': SENSOR_CURRENCY,
          'market': 'USD',
          'apikey': API_KEY,
          'datatype': API_DATATYPE}

logger = get_logger("digital_currency_by_date")


def response_check(response):
    logger.info("Checking HTTP response from API endpoint...")
    logger.info(
        "---------- Currency: {0}, Date: {1}".format(SENSOR_CURRENCY, SENSOR_DATE))

    url_data = response.content
    df = pd.read_csv(io.StringIO(url_data.decode('utf-8')),
                     index_col=None, header=0, nrows=1)
    if len(df.columns) > 1:
        desired_date = datetime.strptime(SENSOR_DATE, '%Y-%m-%d')
        last_available_date = datetime.strptime(
            df.iloc[0]['timestamp'], '%Y-%m-%d')
        if not desired_date > last_available_date:
            logger.info("Data is now available!")
            return True
    logger.info("Data is not yet available...")
    return False


sensor_task = HttpSensor(task_id='currency_date_sensor',
                         endpoint='query',
                         http_conn_id='http_alphavantage',
                         request_params=params,
                         response_check=response_check,
                         poke_interval=900,
                         dag=dag)


def __retrieve_data():
    logger.info("Retriving data...")
    start_time = time.time()

    url_data = requests.get(API_ENDPOINT, params=params).content

    df = pd.read_csv(io.StringIO(url_data.decode('utf-8')),
                     header=0,
                     index_col=['timestamp'],
                     usecols=['timestamp', 'open (USD)', 'close (USD)'],
                     parse_dates=['timestamp'],
                     nrows=30)

    end_time = time.time()
    logger.info(
        "---------- Data retrieved in: {0} seconds".format(end_time - start_time))
    df.to_csv(
        path_or_buf="{0}-{1}.csv".format(SENSOR_CURRENCY, SENSOR_DATE), index=False)

    return df


def __export_plot(df):
    logger.info("Exporting plot to PDF...")
    start_time = time.time()

    title = "Open and close prices of BTC in last 30 days"
    fig = df.plot(title=title, grid=True).get_figure()
    fig.savefig(
        "{0}-last-30d-from-{1}.pdf".format(SENSOR_CURRENCY, SENSOR_DATE))

    end_time = time.time()
    logger.info(
        "---------- Plot exported in: {0} seconds".format(end_time - start_time))


def __log_avg_difference(df):
    logger.info("Logging average difference between open and close prices...")
    start_time = time.time()

    avg_diff = sum(abs(df['open (USD)'] - df['close (USD)'])) / 30
    file_name = "{0}-last-30d-avg-{1}.csv".format(SENSOR_CURRENCY, SENSOR_DATE)
    if not os.path.isfile(file_name):
        with open(file_name, 'w') as fd:
            fd.write("currency code,last date,avg diff last 30d (USD)")
    with open(file_name, 'a') as fd:
        fd.write("\n{0},{1},{2}".format(SENSOR_CURRENCY, SENSOR_DATE, avg_diff))

    end_time = time.time()
    logger.info(
        "---------- Average difference logged in: {0} seconds".format(end_time - start_time))


def plot_and_log(ds, **kwargs):
    df = __retrieve_data()
    __export_plot(df)
    __log_avg_difference(df)


plot_and_log_task = PythonOperator(
    task_id='plot_and_log',
    provide_context=True,
    python_callable=plot_and_log,
    dag=dag,
)


sensor_task >> plot_and_log_task
