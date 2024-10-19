from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from prefect import task
from prefect.blocks.system import Secret


class DatabaseLayer:
    def __init__(self, ):
        self.db_name = 'historical_stock_prices_raw'
        self.connection_string = self.get_connection_string(self.db_name)
        self.engine, self.SessionLocal, self.Base = self.init_db(self, self.connection_string)
        

    def get_connection_string(db_name):
        """Load the secret token and create the connection string."""
        token = Secret.load("mother-duck-token").get()
        return f'duckdb:///md:{db_name}?motherduck_token={token}'
    
    def init_db(connection_string):
        """Initialize the database connection."""
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
    def write_data_to_db(self, data):
        """High-level function to write data into the database."""
        session = self.SessionLocal()
        try:
            session.add(data)
            session.commit()
            print("Data written to the database successfully.")
        except Exception as e:
            session.rollback()
            print(f"Error writing data to the database: {e}")
        finally:
            session.close()