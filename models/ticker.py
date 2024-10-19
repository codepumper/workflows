from sqlalchemy import Column, Integer, String, Sequence
from sqlalchemy.orm import relationship
from common.db_layer import Base


class Ticker(Base):
    __tablename__ = 'ticker'

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    alpaca_symbol = Column(String, nullable=True)
    eodhd_symbol = Column(String, nullable=True)
    yahoo_symbol = Column(String, nullable=True)
    polygon_symbol = Column(String, nullable=True)

    # alpaca_bars = relationship("AlpacaBarData", back_populates="ticker")
    # eodhd_bars = relationship("EODHDBarData", back_populates="ticker")
    polygon_bars = relationship("PolygonBarData", back_populates="ticker")
    # yahoo_bars = relationship("YahooBarData", back_populates="ticker")
    

    
