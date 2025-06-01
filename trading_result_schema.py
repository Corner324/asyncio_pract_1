from pydantic import BaseModel, Field, computed_field
from datetime import date, datetime


class TradingResultModel(BaseModel):
    exchange_product_id: str = Field(..., alias="Код Инструмента")
    exchange_product_name: str = Field(..., alias="Наименование Инструмента")
    delivery_basis_name: str = Field(..., alias="Базис поставки")
    volume: float = Field(..., alias="Объем Договоров в единицах измерения")
    total: float = Field(..., alias="Обьем Договоров, руб.")
    count: int = Field(..., alias="Количество Договоров, шт.")
    date: date
    created_on: datetime
    updated_on: datetime

    @computed_field
    @property
    def oil_id(self) -> str:
        return self.exchange_product_id[:4]

    @computed_field
    @property
    def delivery_basis_id(self) -> str:
        return self.exchange_product_id[4:7]

    @computed_field
    @property
    def delivery_type_id(self) -> str:
        return self.exchange_product_id[-1]
