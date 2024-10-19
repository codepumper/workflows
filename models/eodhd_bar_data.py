from models.bar_data import BarData
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, ForeignKey

class EODHDBarData(BarData):
    __tablename__ = 'eodhd_bar'

    # ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)
    # ticker = relationship("Ticker", back_populates="eodhd_bar")

    @classmethod
    def from_eodhd_response(cls, data: dict):
        """Create an instance from the EODHD response structure"""
        return cls(
            ticker_id=data.get("ticker_id"),
            date=data.get("date"),
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            close=data.get("close"),
            adjusted_close=data.get("adjusted_close"),
            volume=data.get("volume")
        )