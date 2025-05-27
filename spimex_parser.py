import asyncio
import logging
import os
import time
from datetime import date, datetime
from typing import List, Optional, Tuple

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from database import async_engine, SyncSession
from models import SpimexTradingResult
from pydantic import BaseModel, Field, computed_field
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("spimex_parser.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


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


async def get_max_pages(base_url: str, headers: dict) -> int:
    """Получает максимальное количество страниц пагинации."""
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(base_url) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), "html.parser")
                pagination = soup.find("div", class_="bx-pagination-container")
                if not pagination:
                    return 1
                pages = pagination.find_all("li")
                if not pages:
                    return 1
                last_page = pages[-2].text.strip()  # Предпоследний <li> перед "Вперед"
                return int(last_page) if last_page.isdigit() else 1
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при определении количества страниц: {e}")
            return 1


async def fetch_page(page_url: str, headers: dict, retries: int = 3, delay: float = 2.0) -> Optional[str]:
    """Загружает страницу с ретраями."""
    async with aiohttp.ClientSession(headers=headers) as session:
        for attempt in range(retries):
            try:
                async with session.get(page_url) as response:
                    response.raise_for_status()
                    return await response.text()
            except aiohttp.ClientError as e:
                logger.warning(f"Попытка {attempt + 1} не удалась для {page_url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
        logger.error(f"Не удалось загрузить страницу {page_url} после {retries} попыток")
        return None


def parse_page_links(soup: BeautifulSoup, start_date: date, end_date: date, base_url: str) -> List[Tuple[str, date]]:
    """Парсит ссылки на бюллетени с одной страницы."""
    bulletin_urls = []
    links = soup.find_all("a", class_="accordeon-inner__item-title link xls")
    logger.info(f"Найдено {len(links)} ссылок на странице")

    for link in links:
        href = link.get("href")
        if not href:
            logger.debug("Пропущена ссылка без href")
            continue

        href = href.split("?")[0]
        if "/upload/reports/oil_xls/oil_xls_" not in href or not href.endswith(".xls"):
            logger.debug(f"Пропущена ссылка {href}: не соответствует шаблону oil_xls_")
            continue

        try:
            file_date_str = href.split("oil_xls_")[1][:8]
            file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
            if start_date <= file_date <= end_date:
                full_url = href if href.startswith("http") else f"https://spimex.com{href}"
                bulletin_urls.append((full_url, file_date))
                logger.debug(f"Добавлена ссылка: {full_url}, дата: {file_date}")
            else:
                logger.debug(f"Ссылка {href} вне диапазона дат")
        except (IndexError, ValueError) as e:
            logger.warning(f"Не удалось извлечь дату из ссылки {href}: {e}")

    return bulletin_urls


async def get_bulletin_urls(start_date: date, end_date: date) -> List[Tuple[str, date]]:
    """Собирает URL бюллетеней за указанный период с учетом пагинации."""
    base_url = "https://spimex.com/markets/oil_products/trades/results/"
    bulletin_urls = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    max_pages = await get_max_pages(base_url, headers)
    logger.info(f"Найдено {max_pages} страниц пагинации")

    for page in range(1, max_pages + 1):
        page_url = f"{base_url}?page=page-{page}" if page > 1 else base_url
        logger.info(f"Обрабатывается страница {page}: {page_url}")

        html = await fetch_page(page_url, headers)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        page_urls = parse_page_links(soup, start_date, end_date, base_url)
        bulletin_urls.extend(page_urls)

        if page_urls:
            earliest_date = min(date for _, date in page_urls)
            if earliest_date < date(2023, 1, 1):
                logger.info("Достигнута страница с данными до 2023 года, завершаем сбор")
                break

        pagination = soup.find("div", class_="bx-pagination-container")
        if pagination:
            next_page = pagination.find("li", class_="bx-pag-next")
            if not next_page or not next_page.find("a"):
                logger.info("Достигнута последняя страница пагинации")
                break

    logger.info(f"Всего найдено {len(bulletin_urls)} подходящих бюллетеней")
    return bulletin_urls


async def download_bulletin(url: str, output_path: str) -> bool:
    """Загружает бюллетень по указанному URL асинхронно."""
    try:
        if os.path.exists(output_path):
            logger.info(f"Файл {output_path} уже существует, пропускаем загрузку")
            return True
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    f.write(await response.read())
                logger.info(f"Бюллетень загружен: {output_path}")
                return True
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при загрузке бюллетеня {url}: {e}")
        return False


def parse_bulletin(file_path: str, trade_date: date) -> List[dict]:
    """Парсит Excel-бюллетень и возвращает список словарей."""
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None)

        if len(df) <= 6:
            logger.error(f"Файл {file_path} слишком короткий, нет строки с заголовками")
            return []

        headers = df.iloc[6].fillna("").tolist()
        headers_clean = [h.replace("\n", " ").strip() for h in headers[1:]]

        required_columns = {
            "Код Инструмента": "exchange_product_id",
            "Наименование Инструмента": "exchange_product_name",
            "Базис поставки": "delivery_basis_name",
            "Объем Договоров в единицах измерения": "volume",
            "Обьем Договоров, руб.": "total",
            "Количество Договоров, шт.": "count",
        }

        missing_cols = [col for col in required_columns if col not in headers_clean]
        if missing_cols:
            logger.error(f"Отсутствуют столбцы в {file_path}: {missing_cols}")
            return []

        data_rows = []
        for i in range(8, len(df)):
            row = df.iloc[i].tolist()
            if pd.isna(row[1]) or row[1] == "" or row[1] == "Код Инструмента" or row[1].startswith("Код"):
                logger.debug(f"Пропущена строка {i + 1}: содержит пустое значение или заголовок")
                break
            data_rows.append(row[1:])

        if not data_rows:
            logger.warning(f"Нет данных в {file_path} после строки с заголовками")
            return []

        data_df = pd.DataFrame(data_rows, columns=headers_clean)

        for col in ["Объем Договоров в единицах измерения", "Обьем Договоров, руб.", "Количество Договоров, шт."]:
            data_df[col] = data_df[col].replace("-", pd.NA)
            data_df[col] = pd.to_numeric(data_df[col], errors="coerce").fillna(0)

        data_df = data_df[data_df["Количество Договоров, шт."] > 0]
        data_df = data_df[list(required_columns.keys())]
        data_df = data_df[~data_df["Код Инструмента"].str.contains("Итог", case=False, na=False)]

        current_time = pd.to_datetime(datetime.now())
        data_df["date"] = trade_date
        data_df["created_on"] = current_time
        data_df["updated_on"] = current_time

        from pydantic import TypeAdapter

        adapter = TypeAdapter(List[TradingResultModel])
        records = adapter.validate_python(data_df.to_dict(orient="records"))

        result = [record.dict(by_alias=False) for record in records]
        logger.info(f"Спарсено {len(result)} записей из {file_path}")
        return result
    except Exception as e:
        logger.error(f"Ошибка при парсинге {file_path}: {e}")
        return []


async def save_batch(batch: List[dict]) -> None:
    """Сохраняет один батч данных в базу данных с отдельной сессией."""
    async with async_sessionmaker(async_engine)() as session:
        try:
            stmt = insert(SpimexTradingResult).values(batch).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Сохранен батч из {len(batch)} записей")
        except Exception as e:
            logger.error(f"Ошибка при сохранении батча: {e}")
            await session.rollback()


async def process_bulletins_async(start_date: date, end_date: date, output_dir: str = "bulletins") -> None:
    """Обрабатывает бюллетени за указанный период асинхронно."""
    os.makedirs(output_dir, exist_ok=True)

    bulletin_urls = await get_bulletin_urls(start_date, end_date)
    all_records = []

    for url, trade_date in bulletin_urls:
        output_path = os.path.join(output_dir, f"oil_xls_{trade_date.strftime('%Y%m%d')}.xls")
        if await download_bulletin(url, output_path):
            records = parse_bulletin(output_path, trade_date)
            all_records.extend(records)

    if not all_records:
        logger.info("Нет данных для сохранения в базу")
        return

    batch_size = 1000
    batches = [all_records[i : i + batch_size] for i in range(0, len(all_records), batch_size)]

    tasks = [save_batch(batch) for batch in batches]
    await asyncio.gather(*tasks)

    logger.info(f"Сохранено {len(all_records)} записей в {len(batches)} параллельных батчах")


def process_bulletins_sync(start_date: date, end_date: date, output_dir: str = "bulletins") -> None:
    """Обрабатывает бюллетени за указанный период синхронно."""

    def sync_get_max_pages(base_url: str, headers: dict) -> int:
        try:
            response = requests.get(base_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            pagination = soup.find("div", class_="bx-pagination-container")
            if not pagination:
                return 1
            pages = pagination.find_all("li")
            if not pages:
                return 1
            last_page = pages[-2].text.strip()
            return int(last_page) if last_page.isdigit() else 1
        except Exception as e:
            logger.error(f"Ошибка при определении количества страниц: {e}")
            return 1

    def sync_fetch_page(page_url: str, headers: dict, retries: int = 3, delay: float = 2.0) -> Optional[str]:
        for attempt in range(retries):
            try:
                response = requests.get(page_url, headers=headers)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Попытка {attempt + 1} не удалась для {page_url}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
        logger.error(f"Не удалось загрузить страницу {page_url} после {retries} попыток")
        return None

    def sync_get_bulletin_urls(start_date: date, end_date: date) -> List[Tuple[str, date]]:
        base_url = "https://spimex.com/markets/oil_products/trades/results/"
        bulletin_urls = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }

        max_pages = sync_get_max_pages(base_url, headers)
        logger.info(f"Найдено {max_pages} страниц пагинации")

        for page in range(1, max_pages + 1):
            page_url = f"{base_url}?page=page-{page}" if page > 1 else base_url
            logger.info(f"Обрабатывается страница {page}: {page_url}")

            html = sync_fetch_page(page_url, headers)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            page_urls = parse_page_links(soup, start_date, end_date, base_url)
            bulletin_urls.extend(page_urls)

            if page_urls:
                earliest_date = min(date for _, date in page_urls)
                if earliest_date < date(2023, 1, 1):
                    logger.info("Достигнута страница с данными до 2023 года, завершаем сбор")
                    break

            pagination = soup.find("div", class_="bx-pagination-container")
            if pagination:
                next_page = pagination.find("li", class_="bx-pag-next")
                if not next_page or not next_page.find("a"):
                    logger.info("Достигнута последняя страница пагинации")
                    break

        logger.info(f"Всего найдено {len(bulletin_urls)} подходящих бюллетеней")
        return bulletin_urls

    os.makedirs(output_dir, exist_ok=True)
    bulletin_urls = sync_get_bulletin_urls(start_date, end_date)
    all_records = []

    for url, trade_date in bulletin_urls:
        output_path = os.path.join(output_dir, f"oil_xls_{trade_date.strftime('%Y%m%d')}.xls")
        try:
            if os.path.exists(output_path):
                logger.info(f"Файл {output_path} уже существует, пропускаем загрузку")
            else:
                response = requests.get(url)
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Бюллетень загружен: {output_path}")
            records = parse_bulletin(output_path, trade_date)
            all_records.extend(records)
        except requests.RequestException as e:
            logger.error(f"Ошибка при загрузке бюллетеня {url}: {e}")

    if not all_records:
        logger.info("Нет данных для сохранения в базу")
        return

    batch_size = 1000
    batches = [all_records[i : i + batch_size] for i in range(0, len(all_records), batch_size)]

    for batch in batches:
        with SyncSession() as session:
            try:
                stmt = insert(SpimexTradingResult).values(batch).on_conflict_do_nothing()
                session.execute(stmt)
                session.commit()
                logger.info(f"Сохранен батч из {len(batch)} записей")
            except Exception as e:
                logger.error(f"Ошибка при сохранении батча: {e}")
                session.rollback()

    logger.info(f"Сохранено {len(all_records)} записей в {len(batches)} батчах")


def compare_execution_time(start_date: date, end_date: date, output_dir: str = "bulletins") -> None:
    """Сравнивает время выполнения синхронного и асинхронного кода."""
    logger.info("Запуск синхронной версии...")
    start_time = time.time()
    process_bulletins_sync(start_date, end_date, output_dir)
    sync_duration = time.time() - start_time

    logger.info("Запуск асинхронной версии...")
    start_time = time.time()
    asyncio.run(process_bulletins_async(start_date, end_date, output_dir))
    async_duration = time.time() - start_time

    logger.info(f"Синхронное выполнение: {sync_duration:.2f} секунд")
    logger.info(f"Асинхронное выполнение: {async_duration:.2f} секунд")
    logger.info(f"Асинхронный код быстрее на {sync_duration - async_duration:.2f} секунд")


if __name__ == "__main__":
    start_date = date(2023, 4, 22)
    end_date = date(2025, 5, 27)
    compare_execution_time(start_date, end_date)
