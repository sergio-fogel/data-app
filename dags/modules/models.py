from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockValueModel(Base):
    """Stock value data model."""

    __tablename__ = "stock_value"
    Symbol = Column(String, primary_key=True)
    Date = Column(DateTime, primary_key=True)
    Open = Column(Float)
    High = Column(Float)
    Low = Column(Float)
    Close = Column(Float)
    Volume = Column(Integer)
    
    def __repr__(self):
        return f"<StockValue(symbol='{self.Symbol}', date='{self.Date}', open='{self.Open}', high='{self.High}', low='{self.Low}', close='{self.Close}')>"

