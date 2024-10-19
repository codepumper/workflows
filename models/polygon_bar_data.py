from datetime import datetime, timezone
from models.bar_data import BarData
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, ForeignKey

class PolygonBarData(BarData):
    __tablename__ = 'polygon_bar'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)
    ticker = relationship("Ticker", back_populates="polygon_bars")

    @classmethod
    def from_polygon_response(cls, data: dict):
        """Create an instance from the Polygon response structure"""
        result = data["results"][0]
        date_value = datetime.fromtimestamp(result["t"] / 1000, tz=timezone.utc).date()
        
        return cls(
            ticker_id=result.get("T"),
            date=date_value,
            open=result.get("o"),
            high=result.get("h"),
            low=result.get("l"),
            close=result.get("c"),
            adjusted_close=result.get("ac"), # this is constructed manually by running two queries
            volume=int(result.get("v"))
        )