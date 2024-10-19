from datetime import time
import requests
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from models.polygon_bar_data import PolygonBarData
from common.db_layer import init_db, write_data_to_db, SessionLocal
from models.ticker import Ticker

def construct_polygon_url(symbol, api_key, adjusted=True):
    base_url = 'https://api.polygon.io/v2/aggs/ticker'
    adjusted_param = 'true' if adjusted else 'false'
    url = f'{base_url}/{symbol}/prev?adjusted={adjusted_param}&apiKey={api_key}'
    return url

@task(retries=3, retry_delay_seconds=15)
def fetch_polygon_data(symbol, api_key, adjusted=False):
    logger = get_run_logger()
    url = construct_polygon_url(symbol, api_key, adjusted)
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Polygon API: {e}")
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response from Polygon API: {e}")

@flow
def run_polygon_data_pipeline():
    logger = get_run_logger()
    api_key = Secret.load("polygon-api-key").get()

    _, SessionLocal, _ = init_db()

    session = SessionLocal()

    symbols = session.query(Ticker.polygon_symbol).filter(Ticker.polygon_symbol.isnot(None)).all()
    symbols = [symbol[0] for symbol in symbols]  

    new_data_added = False

    for symbol in symbols:
        try:
            not_adjusted_data = fetch_polygon_data(symbol, api_key)
            adjusted = fetch_polygon_data(symbol, api_key, adjusted=True)

            if not not_adjusted_data or not adjusted:
                logger.warning(f"No data returned for symbol: {symbol}")
                continue

            not_adjusted_data['results'][0]['ac'] = adjusted['results'][0]['c']
            
            bar_data = PolygonBarData.from_polygon_response(not_adjusted_data)
            existing_data = session.query(PolygonBarData).filter_by(ticker_id=bar_data.ticker_id, date=bar_data.date).first()
            if existing_data:
                logger.info(f"Data for {symbol} on {bar_data.date} already exists. Skipping.")
                continue

            write_data_to_db(bar_data)
            new_data_added = True
            time.sleep(30)
        except Exception as e:
            logger.error(f"Error processing data for symbol: {symbol}. Error: {e}")

    if not new_data_added:
        logger.warning("No new data was added to the database.")
        raise Exception("Pipeline did not add any new data.")

    logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    run_polygon_data_pipeline()
    