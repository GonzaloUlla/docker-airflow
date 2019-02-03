import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import requests
import io
import logging
import time
from datetime import timedelta, date


SYMBOL_LIST_URL = "https://www.alphavantage.co/digital_currency_list/"
API_ENDPOINT = "https://www.alphavantage.co/query"
API_FUNCTION = "DIGITAL_CURRENCY_DAILY"
API_DATATYPE = "csv"
API_KEY = ["V26HZ0GFH4GWYJPG"]
# OLD_API_KEYS = ["HE6HPTT0QFHG2ZYY", "OWGOAH1MLEK1J3IA", "B4YVATR33W46TE6R", "PMCQ61RYQ7T7TUFY"]

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2, hour=12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='digital_currency_daily',
    default_args=args,
    schedule_interval=timedelta(days=1),
)


def get_logger(prefix):
    logFormatter = logging.Formatter(
        "[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s")
    logger = logging.getLogger(prefix)

    fileHandler = logging.FileHandler(
        "/usr/local/airflow/logs/{0}_{1}.log".format(prefix, str(date.today())))
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    return logger


logger = get_logger("digital_currency_daily")


def __read_currencies():
    logger.info("Reading digital currencies...")
    return pd.read_csv(SYMBOL_LIST_URL)


def __retrieve_data(symbol):
    params = {'function': API_FUNCTION, 'market': 'USD', 'apikey': API_KEY,
              'datatype': API_DATATYPE}  # Only USD
    limit = 5  # "500 API requests per day"
    symbol = symbol.head(limit)
    start_time = time.time()
    logger.info(
        "Starting to read first {0} digital currencies...".format(limit))
    list_ = []

    for s_index, s_row in symbol.iterrows():
        params['symbol'] = s_row['currency code']
        url_data = requests.get(API_ENDPOINT, params=params).content
        df = pd.read_csv(io.StringIO(url_data.decode('utf-8')),
                         index_col=None, header=0, nrows=1)
        if len(df.columns) > 1:
            df.insert(0, 'currency code',
                      s_row['currency code'], allow_duplicates=False)
            list_.append(df)
            logger.info("Added data from currency: {0}".format(
                s_row['currency code']))
        else:
            logger.info("Error calling API endpoint: {0}".format(url_data))

        end_time = time.time()
        elapsed_time = end_time - start_time
        remaining_time = limit * 13 - elapsed_time

        logger.info("---------- Index: {0}, Elapsed time: {1}, Estimated remaining time: {2}".format(
            s_index, elapsed_time, remaining_time))
        logger.info("Sleeping for 12 seconds")
        time.sleep(12)  # 5 API requests per minute at most

    return pd.concat(list_, axis=0, ignore_index=True)


def __remove_duplicated_columns(frame):
    logger.info("Removing duplicated columns...")
    frame.drop(['open (USD).1', 'high (USD).1', 'low (USD).1',
                'close (USD).1'], axis=1, inplace=True)
    return frame


def __export_to_csv(frame_to_export):
    logger.info("Exporting dataframe to csv...")
    timestamp = frame_to_export.iloc[0]['timestamp']
    frame_to_export.to_csv(
        path_or_buf="{0}.csv".format(timestamp), index=False)


def read_and_export(ds, **kwargs):
    symbol = __read_currencies()
    frame = __retrieve_data(symbol)
    frame_to_export = __remove_duplicated_columns(frame)
    __export_to_csv(frame_to_export)


read_and_export_task = PythonOperator(
    task_id='read_and_export',
    provide_context=True,
    python_callable=read_and_export,
    dag=dag,
)
