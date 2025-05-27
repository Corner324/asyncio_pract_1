from datetime import date

from spimex_parser import compare_execution_time

if __name__ == "__main__":
    start_date = date(2023, 4, 22)
    end_date = date(2025, 5, 27)  # Ограничиваем текущей датой
    compare_execution_time(start_date, end_date)
