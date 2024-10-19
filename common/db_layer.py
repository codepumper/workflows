from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from prefect import task
from prefect.blocks.system import Secret
import asyncio

db_name = 'historical_stock_prices_raw'

async def get_connection_string():
    """Load the secret token and create the connection string."""
    secret_block = await Secret.load("mother-duck-token")
    token = secret_block.get()
    return f'duckdb:///md:{db_name}?motherduck_token={token}'

async def init_db():
    """Initialize the database connection."""
    connection_string = await get_connection_string()
    engine = create_engine(connection_string)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
    return engine, SessionLocal, Base

def create_tables(engine, Base):
    """Create tables in the database."""
    from models.polygon_bar_data import PolygonBarData
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

# Initialize the database
engine, SessionLocal, Base = asyncio.run(init_db())