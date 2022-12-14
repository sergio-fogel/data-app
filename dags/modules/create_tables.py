from sqlalchemy import create_engine, MetaData
from models import Base, StockValueModel


def main():
    """Program entrypoint."""
    # Logic to create tables goes here.
    # https://docs.sqlalchemy.org/en/14/orm/tutorial.html#create-a-schema
    

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/stocks', pool_size=10, max_overflow=-1, pool_timeout=300)

    metadata = MetaData(engine)

    class StockValue(StockValueModel, Base):
        __tablename__ = "stock_value"

    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()

