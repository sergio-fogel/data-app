from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockValueModel(Base):
    """Stock value data model."""

    __tablename__ = "stock_value"
    symbol = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    def __repr__(self):
        return f"<StockValue(Symbol='{self.symbol}', Date='{self.date}', Open='{self.open}', High='{self.high}', Low='{self.low}', Close='{self.close}', Volume='{self.volume}')>"

