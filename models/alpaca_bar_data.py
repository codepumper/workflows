from models.bar_data import BarData
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, ForeignKey

class AlpacaBarData(BarData):
    __tablename__ = 'alpaca_bar'

    # ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)
    # ticker = relationship("Ticker", back_populates="alpaca_bars")

    @classmethod
    def from_alpaca_response(cls, data: dict):
        """Create an instance from the Alpaca response structure"""
        return cls(
            #ticker_id=data.get("ticker_id"),
            date=data.get("date"),
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            close=data.get("close"),
            adjusted_close=data.get("adjusted_close"),
            volume=data.get("volume")
        )