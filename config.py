import os

from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

# Отладочный вывод
if __name__ == "__main__":
    print(f"DB_NAME: {DB_NAME}")
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DB_USER: {DB_USER}")
    print(f"DB_PASS: {'[HIDDEN]' if DB_PASS else None}")
