from datetime import datetime

from sqlalchemy import Date, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseModel(DeclarativeBase):
    pass


class SpimexTradingResult(BaseModel):
    __tablename__ = "spimex_trading_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str] = mapped_column(String, nullable=False)
    exchange_product_name: Mapped[str] = mapped_column(String, nullable=False)
    oil_id: Mapped[str] = mapped_column(String, nullable=False)
    delivery_basis_id: Mapped[str] = mapped_column(String, nullable=False)
    delivery_basis_name: Mapped[str] = mapped_column(String, nullable=False)
    delivery_type_id: Mapped[str] = mapped_column(String, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    total: Mapped[float] = mapped_column(Float, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    created_on: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_on: Mapped[datetime] = mapped_column(DateTime, nullable=False)
