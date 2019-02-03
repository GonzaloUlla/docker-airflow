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

from digital_currency_daily import OUTPUTS_FOLDER, API_ENDPOINT, API_FUNCTION, API_DATATYPE, API_KEY, get_logger


SENSOR_CURRENCY = os.environ['SENSOR_CURRENCY']
SENSOR_DATE = os.environ['SENSOR_DATE']

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(9, hour=12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='digital_currency_by_date',
    default_args=args,
    schedule_interval='@weekly'
)

params = {'function': API_FUNCTION,
          'symbol': SENSOR_CURRENCY,
          'market': 'USD',
          'apikey': API_KEY,
          'datatype': API_DATATYPE}

logger = get_logger("digital_currency_by_date")


def response_check(response):
    logger.info("---------- Checking HTTP response from API endpoint...")
    logger.info("Currency: {0}, Date: {1}".format(
        SENSOR_CURRENCY, SENSOR_DATE))

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
    logger.info("Data is not available yet. Sleeping for 30 minutes...")
    return False


def retrieve_data():
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
        "Data retrieved in: {0} seconds".format(end_time - start_time))
    file_name = "{0}-last-30d-aux-{1}.csv".format(SENSOR_CURRENCY, SENSOR_DATE)
    df.to_csv(path_or_buf="{0}{1}".format(
        OUTPUTS_FOLDER, file_name), index=False)
    if os.path.isfile("{0}{1}".format(OUTPUTS_FOLDER, file_name)):
        logger.info("Pre-processing file {0} exported.".format(file_name))
    return df


def export_plot(**context):
    logger.info("Exporting plot to PDF...")
    start_time = time.time()

    df = context['task_instance'].xcom_pull(task_ids='retrieve_data')
    title = "Open and close prices of BTC in last 30 days"
    fig = df.plot(title=title, grid=True).get_figure()
    file_name = "{0}-last-30d-plot-{1}.pdf".format(
        SENSOR_CURRENCY, SENSOR_DATE)
    fig.savefig("{0}{1}".format(OUTPUTS_FOLDER, file_name))
    end_time = time.time()
    if os.path.isfile("{0}{1}".format(OUTPUTS_FOLDER, file_name)):
        logger.info(
            "Plot {0} exported in: {1} seconds".format(file_name, end_time - start_time))


def log_avg_difference(**context):
    logger.info("Logging average difference between open and close prices...")
    start_time = time.time()

    df = context['task_instance'].xcom_pull(task_ids='retrieve_data')
    avg_diff = sum(abs(df['open (USD)'] - df['close (USD)'])) / 30
    file_name = "avg-diff-open-close-last-30d.csv"
    if not os.path.isfile("{0}{1}".format(OUTPUTS_FOLDER, file_name)):
        with open("{0}{1}".format(OUTPUTS_FOLDER, file_name), 'w') as fd:
            fd.write("currency code,last date,avg diff last 30d (USD)")
    with open("{0}{1}".format(OUTPUTS_FOLDER, file_name), 'a') as fd:
        fd.write("\n{0},{1},{2}".format(
            SENSOR_CURRENCY, SENSOR_DATE, avg_diff))

    end_time = time.time()
    if os.path.isfile("{0}{1}".format(OUTPUTS_FOLDER, file_name)):
        logger.info("AVG difference logged to {0} in: {1} seconds".format(
            file_name, end_time - start_time))


sensor_task = HttpSensor(task_id='currency_date_sensor',
                         endpoint='query',
                         http_conn_id='http_alphavantage',
                         request_params=params,
                         response_check=response_check,
                         poke_interval=1800,  # Each 30 minutes, 1 week timeout, 48 requests/day
                         dag=dag)


retrieve_data_task = PythonOperator(
    task_id='retrieve_data',
    provide_context=False,
    python_callable=retrieve_data,
    dag=dag,
)


export_plot_task = PythonOperator(
    task_id='export_plot',
    provide_context=True,
    python_callable=export_plot,
    dag=dag,
)


log_avg_difference_task = PythonOperator(
    task_id='log_avg_difference',
    provide_context=True,
    python_callable=log_avg_difference,
    dag=dag,
)


sensor_task >> retrieve_data_task >> [
    export_plot_task, log_avg_difference_task]
