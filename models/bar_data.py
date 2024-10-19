from sqlalchemy import Column, DateTime, Integer, Float, ForeignKey, Date, Sequence, UniqueConstraint, func
from common.db_layer import Base

class BarData(Base):
    __abstract__ = True

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    ticker = Column(Integer, ForeignKey('ticker.id'), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    adjusted_close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=func.now())

    #__table_args__ = (UniqueConstraint('ticker', 'date', name='_ticker_date_uc'),)