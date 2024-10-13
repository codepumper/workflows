import requests
import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.exceptions import PrefectException
from prefect.blocks.system import Secret
from time import sleep

@task(retries=3, retry_delay_seconds=10)
def fetch_eod_data(symbol, api_token):
    logger = get_run_logger()
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = f'https://eodhd.com/api/eod/{symbol}?from={yesterday}&to={yesterday}&period=d&api_token={api_token}&fmt=json'
    logger.info(f"Fetching data for {symbol} from {yesterday}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Data fetched successfully for {symbol}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response for {symbol}: {e}")

@task(retries=2, retry_delay_seconds=5)
def validate_eod_data(data, required_fields):
    logger = get_run_logger()
    try:
        for record in data:
            for field in required_fields:
                if field not in record:
                    logger.error(f"Error: Missing required field '{field}' in record: {record}")
                    return False
        logger.info("Data validation successful")
        return True
    except Exception as e:
        logger.error(f"Error validating data: {e}")

@task
def setup_eod_table(con):
    logger = get_run_logger()
    try:
        create_sequence_query = """
        CREATE SEQUENCE IF NOT EXISTS eodhd_raw_id_seq;
        """
        con.execute(create_sequence_query)
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS eodhd_raw (
            id INTEGER DEFAULT nextval('eodhd_raw_id_seq') PRIMARY KEY,
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adjusted_close FLOAT,
            volume INTEGER,
            symbol STRING
        );
        """
        con.execute(create_table_query)
        logger.info("Table created or already exists")
    except Exception as e:
        logger.error(f"Error setting up table: {e}")

@task(retries=2, retry_delay_seconds=5)
def store_eod_data(con, data, symbol):
    logger = get_run_logger()
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        check_query = """
        SELECT COUNT(*) FROM eodhd_raw WHERE date = ? AND symbol = ?;
        """
        result = con.execute(check_query, (yesterday, symbol)).fetchone()
        
        if result[0] > 0:
            logger.info(f"Data for {symbol} on {yesterday} already exists. Skipping insertion.")
            return
        
        insert_query = """
        INSERT INTO eodhd_raw (date, open, high, low, close, adjusted_close, volume, symbol) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        records = [
            (
                datetime.strptime(record['date'], '%Y-%m-%d'),
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['adjusted_close'],
                record['volume'],
                symbol
            )
            for record in data
        ]
        con.executemany(insert_query, records)
        logger.info(f"Data inserted successfully for {symbol}")
    except Exception as e:
        logger.error(f"Error storing data for {symbol}: {e}")

@flow
def run_eodhd_data_pipeline():
    logger = get_run_logger()
    
    api_token = Secret.load("eodhd-api-key").get()
    motherduck_token = Secret.load("mother-duck-token").get()

    db_name = 'historical_stock_prices_raw'
    db_path = f"md:{db_name}?motherduck_token={motherduck_token}"

    symbols = ['AAPL.US']
    required_fields = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']

    try:
        con = duckdb.connect(db_path)
    except Exception as e:
        logger.error(f"Error: Failed to connect to DuckDB. {e}")
        return

    setup_eod_table(con)

    for symbol in symbols:
        data = fetch_eod_data(symbol, api_token)
        if data and validate_eod_data(data, required_fields):
            store_eod_data(con, data, symbol)

    logger.info("Data pipeline completed successfully")

if __name__ == "__main__":
    run_eodhd_data_pipeline()