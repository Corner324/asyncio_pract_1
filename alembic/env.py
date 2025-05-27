import os
import sys
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import create_engine, pool
import logging

# Загружаем переменные окружения
load_dotenv()

# Добавляем путь к проекту, чтобы импортировать database и models
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database import DATABASE_URL
from models import BaseModel, SpimexTradingResult

# Конфигурация Alembic
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Метадата моделей
target_metadata = BaseModel.metadata
logger = logging.getLogger('alembic.runtime.migration')
logger.info("=== Alembic sees the following tables ===")
for tname in target_metadata.tables:
    logger.info(f" - {tname}")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = create_engine(DATABASE_URL, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    print("💡 Зарегистрированные таблицы:", BaseModel.metadata.tables.keys())
    run_migrations_offline()
else:
    run_migrations_online()
