from prefect import flow, task, get_run_logger
from prefect_duckdb import DuckDBConnector

@task
def read_tickers():
    logger = get_run_logger()

    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InJvYmVydGtvdmFjcy5hdEBwcm90b25tYWlsLmNvbSIsInNlc3Npb24iOiJyb2JlcnRrb3ZhY3MuYXQucHJvdG9ubWFpbC5jb20iLCJwYXQiOiJjUjJweWRSRm5OMVh3WEJWTTdLVFQzYzBpUlZyWjZlSGUxRFFHcmh6YjFRIiwidXNlcklkIjoiYTk1NzRjYmEtODYzMC00MTA1LWFjMGItMDQ3NDZlNGZiOGU3IiwiaXNzIjoibWRfcGF0IiwiaWF0IjoxNzI4Njg2MDczfQ.MQ4kAmjR7gdPhsnsooLPiILKjsTjBezlDEbnD9hGoLg'
    
    cs = f'duckdb:///md:historical_stock_prices_raw?motherduck_token={token}'
    duckdb_connector = DuckDBConnector(connection_string=cs)

    duckdb_connector.save(name="my-duckdb-connector")

    duckdb_connector = DuckDBConnector.load("my-duckdb-connector")

    query = "SELECT * FROM tickers"
    with duckdb_connector.get_connection() as conn:
        result = conn.execute(query).fetchall()
        logger.info(f"Read {len(result)} rows from tickers table")
        return result

@flow
def test_workflow():
    tickers_data = read_tickers()
    logger = get_run_logger()
    logger.info(f"Tickers data: {tickers_data}")

if __name__ == "__main__":
    test_workflow()