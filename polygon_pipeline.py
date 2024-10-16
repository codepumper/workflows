import requests
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

@task(retries=3, retry_delay_seconds=10)
def fetch_polygon_data(symbol, api_key):
    logger = get_run_logger()
    url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={api_key}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info("Data fetched successfully for AAPL")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Polygon API: {e}")
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response from Polygon API: {e}")

@flow
def run_polygon_data_pipeline():
    logger = get_run_logger()
    api_key = Secret.load("polygon-api-key").get()

    symbols = ['AAPL']

    for symbol in symbols:
        data = fetch_polygon_data(symbol, api_key)
        logger.info(data)

    print("Pipeline completed")

if __name__ == "__main__":
    run_polygon_data_pipeline()
    