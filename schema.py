from pydantic import BaseModel
from datetime import datetime, date

class OHLCVData(BaseModel):
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    adjusted_close: float
    volume: int
    created_at: datetime = datetime.now()

    @classmethod
    def from_first_response(cls, data: dict):
        """Create an instance from the first response structure"""
        return cls(
            ticker=data.get("ticker"),
            date=data.get("date"),
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            close=data.get("close"),
            adjusted_close=data.get("adjusted_close"),
            volume=data.get("volume")
        )

    @classmethod
    def from_second_response(cls, data: dict):
        """Create an instance from the second response structure"""
        result = data["results"][0]  # Take the first result
        # Convert timestamp to date (assuming timestamp is in milliseconds)
        date_value = datetime.utcfromtimestamp(result["t"] / 1000).date()
        
        return cls(
            ticker=result.get("T"),
            date=date_value,
            open=result.get("o"),
            high=result.get("h"),
            low=result.get("l"),
            close=result.get("c"),
            adjusted_close=result.get("c"),  # In the second response, 'adjusted_close' maps to 'close'
            volume=int(result.get("v"))
        )

# Example first response data
ohlcv_data = {
    "ticker": "AAPL",
    "date": "2024-10-14",
    "open": 305.15,
    "high": 310.42,
    "low": 304.5,
    "close": 309.84,
    "adjusted_close": 309.84,
    "volume": 2376300
}

# Example second response data
response_data = {
    "ticker": "AAPL",
    "queryCount": 1,
    "resultsCount": 1,
    "adjusted": True,
    "results": [
        {
            "T": "AAPL",
            "v": 6.0799239e+07,
            "vw": 234.85,
            "o": 233.61,
            "c": 233.85,
            "h": 237.49,
            "l": 232.37,
            "t": 1729022400000,
            "n": 827448
        }
    ],
    "status": "OK",
    "request_id": "e53570ff10373ae4558b11c97ca2a804",
    "count": 1
}

# Creating instances from both response formats
financial_data_1 = OHLCVData.from_first_response(ohlcv_data)
financial_data_2 = OHLCVData.from_second_response(response_data)

# Output the instances
print(financial_data_1)
print(financial_data_2)
