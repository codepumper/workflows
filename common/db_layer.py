from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from prefect import task
from prefect.blocks.system import Secret

def init_db():
    # Load the secret token
    token = Secret.load("mother-duck-token").get()
    
    # Create the connection string
    connection_string = f'duckdb:///md:{db_name}?motherduck_token={token}'
    
    # Create the engine
    engine = create_engine(connection_string)
    
    # Create the session
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create the base
    Base = declarative_base()
    
    return engine, SessionLocal, Base

def create_tables():
    # from models.alpaca_bar_data import AlpacaBarData
    from models.polygon_bar_data import PolygonBarData
    # from models.eodhd_bar_data import EODHDBarData
    # from models.yahoo_bar_data import YahooBarData
    from models.ticker import Ticker
    
    engine, _, Base = init_db()
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