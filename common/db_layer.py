from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from prefect import task
from prefect.blocks.system import Secret

# TODO  - refactor this file
db_name = 'historical_stock_prices_raw'

token = Secret.load("mother-duck-token").get()
connection_string = f'duckdb:///md:{db_name}?motherduck_token={token}'

engine = create_engine(connection_string)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def create_tables():
    # from models.alpaca_bar_data import AlpacaBarData
    from models.polygon_bar_data import PolygonBarData
    # from models.eodhd_bar_data import EODHDBarData
    # from models.yahoo_bar_data import YahooBarData
    from models.ticker import Ticker
    
    Base.metadata.create_all(bind=engine)

@task
def write_data_to_db(data):
    """High-level function to write data into the database."""
    session = SessionLocal()
    try:
        session.add(data)
        session.commit()
        print("Data written to the database successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error writing data to the database: {e}")
    finally:
        session.close()