import logging

from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from models import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("database.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

SYNC_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    # Синхронный движок для создания таблиц и синхронных операций
    sync_engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
    BaseModel.metadata.create_all(sync_engine, checkfirst=True)
    logger.info("Таблицы созданы или уже существуют")

    # Асинхронный движок для асинхронных операций
    async_engine = create_async_engine(ASYNC_DATABASE_URL, echo=False)

    # Синхронная сессия
    SyncSession = sessionmaker(bind=sync_engine)

    # Асинхронная сессия
    AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

except Exception as e:
    logger.error(f"Ошибка подключения к базе данных: {e}")
    raise
