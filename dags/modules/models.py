from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockValueModel(Base):
    """Stock value data model."""

    __tablename__ = "stock_value"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adjusted_close = Column(Float)
    volume = Column(Integer)
    dividend_amount = Column(Float)
    split_coefficient = Column(Float)
    
    def __repr__(self):
        return f"<StockValue(symbol='{self.symbol}', date='{self.date}', open='{self.open}', high='{self.high}', low='{self.low}', close='{self.close}')>"

