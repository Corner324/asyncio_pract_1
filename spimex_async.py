import asyncio
import logging
import os
import time
from datetime import date, datetime
from typing import List, Optional, Tuple
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from database import async_engine
from models import SpimexTradingResult
from pydantic import BaseModel, Field, computed_field
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

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


def parse_page_links(soup: BeautifulSoup, start_date: date, end_date: date, base_url: str) -> List[Tuple[str, date]]:
    """Парсит ссылки на бюллетени с одной страницы."""
    start_time = time.time()
    bulletin_urls = []
    links = soup.find_all("a", class_="accordeon-inner__item-title link xls")
    logger.info(f"Найдено {len(links)} ссылок на странице за {time.time() - start_time:.2f} секунд")

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


def parse_bulletin(file_path: str, trade_date: date) -> List[dict]:
    start_time = time.time()
    try:
        # Проверка только расширения файла
        if not file_path.lower().endswith(".xls"):
            logger.error(f"Файл {file_path} не имеет расширение .xls")
            return []

        # Чтение файла через pandas
        try:
            df = pd.read_excel(file_path, sheet_name=0, header=None)
        except Exception as e:
            logger.error(f"Не удалось открыть файл {file_path} как Excel: {e}")
            return []

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

        logger.debug(f"До фильтрации: {len(data_df)} строк")
        data_df = data_df[data_df["Количество Договоров, шт."] > 0]
        logger.debug(f"После фильтрации 'Количество Договоров, шт.' > 0: {len(data_df)} строк")
        data_df = data_df[list(required_columns.keys())]
        data_df = data_df[~data_df["Код Инструмента"].str.contains("Итог", case=False, na=False)]
        logger.debug(f"После фильтрации 'Итог': {len(data_df)} строк")

        current_time = pd.to_datetime(datetime.now())
        data_df["date"] = trade_date
        data_df["created_on"] = current_time
        data_df["updated_on"] = current_time

        from pydantic import TypeAdapter

        adapter = TypeAdapter(List[TradingResultModel])
        records = adapter.validate_python(data_df.to_dict(orient="records"))

        result = [record.dict(by_alias=False) for record in records]
        logger.info(f"Спарсено {len(result)} записей из {file_path} за {time.time() - start_time:.2f} секунд")
        return result
    except Exception as e:
        logger.error(f"Ошибка при парсинге {file_path}: {e}")
        return []


async def get_max_pages(base_url: str, headers: dict) -> int:
    """Получает максимальное количество страниц пагинации."""
    start_time = time.time()
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(base_url) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), "html.parser")
                pagination = soup.find("div", class_="bx-pagination-container")
                if not pagination:
                    logger.info("Пагинация не найдена, возвращаем 1")
                    return 1
                pages = pagination.find_all("li")
                if not pages:
                    logger.info("Список страниц пуст, возвращаем 1")
                    return 1
                last_page = pages[-2].text.strip()
                result = int(last_page) if last_page.isdigit() else 1
                logger.info(f"Найдено {result} страниц пагинации за {time.time() - start_time:.2f} секунд")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при определении количества страниц: {e}")
            return 1


async def fetch_page(page_url: str, headers: dict, retries: int = 3, delay: float = 2.0) -> Optional[str]:
    """Загружает страницу с ретраями и задержкой для предотвращения тротлинга."""
    start_time = time.time()
    async with aiohttp.ClientSession(headers=headers) as session:
        for attempt in range(retries):
            try:
                async with session.get(page_url) as response:
                    response.raise_for_status()
                    await asyncio.sleep(0.5)  # для предотвращения троттлинга
                    content = await response.text()
                    logger.debug(f"Страница {page_url} загружена за {time.time() - start_time:.2f} секунд")
                    return content
            except aiohttp.ClientError as e:
                logger.warning(f"Попытка {attempt + 1} не удалась для {page_url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
        logger.error(f"Не удалось загрузить страницу {page_url} после {retries} попыток")
        return None


async def get_bulletin_urls(start_date: date, end_date: date) -> List[Tuple[str, date]]:
    """Собирает URL бюллетеней за указанный период с учетом пагинации."""
    start_time = time.time()
    base_url = "https://spimex.com/markets/oil_products/trades/results/"
    bulletin_urls = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    max_pages = await get_max_pages(base_url, headers)

    semaphore = asyncio.Semaphore(10)  # Ограничение на 10 одновременных запросов

    async def fetch_page_with_semaphore(page):
        async with semaphore:
            page_url = f"{base_url}?page=page-{page}" if page > 1 else base_url
            logger.info(f"Обрабатывается страница {page}: {page_url}")
            return await fetch_page(page_url, headers)

    tasks = [fetch_page_with_semaphore(page) for page in range(1, max_pages + 1)]
    pages = await asyncio.gather(*tasks)

    for html in pages:
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

    logger.info(f"Всего найдено {len(bulletin_urls)} подходящих бюллетеней за {time.time() - start_time:.2f} секунд")
    return bulletin_urls


async def download_bulletin(url: str, output_path: str) -> bool:
    """Загружает бюллетень по указанному URL асинхронно."""
    start_time = time.time()
    try:
        if os.path.exists(output_path):
            logger.info(f"Файл {output_path} уже существует, пропускаем загрузку")
            return True

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                with open(output_path, "wb") as f:
                    f.write(content)
                logger.info(f"Бюллетень загружен: {output_path} за {time.time() - start_time:.2f} секунд")
                return True
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при загрузке бюллетеня {url}: {e}")
        return False


async def save_batch(batch: List[dict]) -> None:
    """Сохраняет один батч данных в базу данных с отдельной сессией."""
    start_time = time.time()
    async with async_sessionmaker(async_engine)() as session:
        try:
            stmt = insert(SpimexTradingResult).values(batch).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Сохранен батч из {len(batch)} записей за {time.time() - start_time:.2f} секунд")
        except Exception as e:
            logger.error(f"Ошибка при сохранении батча: {e}")
            await session.rollback()


async def process_bulletins_async(start_date: date, end_date: date, output_dir: str = "bulletins") -> None:
    """Обрабатывает бюллетени за указанный период асинхронно."""
    start_time = time.time()
    os.makedirs(output_dir, exist_ok=True)

    if end_date > date.today():
        logger.warning(f"Конец диапазона дат ({end_date}) в будущем, устанавливаем текущую дату")
        end_date = date.today()

    bulletin_urls = await get_bulletin_urls(start_date, end_date)
    all_records = []

    semaphore = asyncio.Semaphore(10)

    async def download_with_semaphore(url, trade_date):
        async with semaphore:
            output_path = os.path.join(output_dir, f"oil_xls_{trade_date.strftime('%Y%m%d')}.xls")
            if await download_bulletin(url, output_path):
                records = parse_bulletin(output_path, trade_date)
                return records
            return []

    tasks = [download_with_semaphore(url, trade_date) for url, trade_date in bulletin_urls]
    results = await asyncio.gather(*tasks)
    for records in results:
        all_records.extend(records)

    if not all_records:
        logger.info("Нет данных для сохранения в базу")
        return

    batch_size = 1000
    batches = [all_records[i : i + batch_size] for i in range(0, len(all_records), batch_size)]

    for batch in batches:
        await save_batch(batch)

    logger.info(
        f"Сохранено {len(all_records)} записей в {len(batches)} батчах за {time.time() - start_time:.2f} секунд"
    )


if __name__ == "__main__":
    start_date = date(2023, 4, 22)
    end_date = date(2023, 4, 30)
    asyncio.run(process_bulletins_async(start_date, end_date))
