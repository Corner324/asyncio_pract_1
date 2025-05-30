import asyncio
import logging
from datetime import date
import time
from spimex_sync import process_bulletins_sync
from spimex_async import process_bulletins_async

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compare_execution_time(start_date: date, end_date: date, output_dir: str = "bulletins") -> None:
    """Compares execution time of synchronous and asynchronous bulletin processing."""
    # Run synchronous version
    logger.info("Запуск синхронной версии...")
    sync_start = time.time()
    process_bulletins_sync(start_date, end_date, output_dir)
    sync_duration = time.time() - sync_start

    # Run asynchronous version
    logger.info("Запуск асинхронной версии...")
    async_start = time.time()
    asyncio.run(process_bulletins_async(start_date, end_date, output_dir))
    async_duration = time.time() - async_start

    # Log results
    logger.info(f"Синхронное выполнение: {sync_duration:.2f} секунд")
    logger.info(f"Асинхронное выполнение: {async_duration:.2f} секунд")

    # Calculate and display percentage difference
    if sync_duration > async_duration:
        percent_faster = ((sync_duration - async_duration) / sync_duration) * 100
        logger.info(f"Асинхронный код быстрее на {sync_duration - async_duration:.2f} секунд ({percent_faster:.2f}%)")
    elif async_duration > sync_duration:
        percent_faster = ((async_duration - sync_duration) / async_duration) * 100
        logger.info(f"Синхронный код быстрее на {async_duration - sync_duration:.2f} секунд ({percent_faster:.2f}%)")
    else:
        logger.info("Оба подхода выполнены за одинаковое время")


if __name__ == "__main__":
    start_date = date(2023, 4, 22)
    end_date = date(2025, 5, 27)  # Ограничиваем текущей датой
    compare_execution_time(start_date, end_date)
