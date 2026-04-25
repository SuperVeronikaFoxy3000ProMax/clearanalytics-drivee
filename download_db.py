"""Загрузка CSV в MySQL/MariaDB: incity, pass_detail, driver_detail. См. notes.md. """

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent

DDL_INCITY = """
CREATE TABLE IF NOT EXISTS incity (
    city_id INT,
    offset_hours INT,
    order_id VARCHAR(64),
    tender_id VARCHAR(64),
    user_id VARCHAR(64),
    driver_id VARCHAR(64),
    status_order VARCHAR(32),
    status_tender VARCHAR(32),
    order_timestamp DATETIME,
    tender_timestamp DATETIME,
    driveraccept_timestamp DATETIME,
    driverarrived_timestamp DATETIME,
    driverstarttheride_timestamp DATETIME,
    driverdone_timestamp DATETIME,
    clientcancel_timestamp DATETIME,
    drivercancel_timestamp DATETIME,
    order_modified_local DATETIME,
    cancel_before_accept_local DATETIME,
    distance_in_meters DOUBLE,
    duration_in_seconds DOUBLE,
    price_order_local DOUBLE,
    price_tender_local DOUBLE,
    price_start_local DOUBLE
)"""

DDL_PASS = """
CREATE TABLE IF NOT EXISTS pass_detail (
    city_id INT,
    user_id VARCHAR(64),
    order_date_part DATE,
    user_reg_date DATE,
    orders_count INT,
    orders_cnt_with_tenders INT,
    orders_cnt_accepted INT,
    rides_count INT,
    client_cancel_after_accept INT,
    rides_time_sum_seconds DOUBLE,
    online_time_sum_seconds DOUBLE
)"""

DDL_DRIVER = """
CREATE TABLE IF NOT EXISTS driver_detail (
    city_id INT,
    driver_id VARCHAR(64),
    tender_date_part DATE,
    driver_reg_date DATE,
    `orders` INT,
    orders_cnt_with_tenders INT,
    orders_cnt_accepted INT,
    rides_count INT,
    client_cancel_after_accept INT,
    rides_time_sum_seconds DOUBLE,
    online_time_sum_seconds DOUBLE
)"""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Нет файла: {path}")
    return pd.read_csv(path, low_memory=False)


def _parse_incity_dates(df: pd.DataFrame) -> None:
    for col in (
        "order_timestamp", "tender_timestamp", "driveraccept_timestamp", "driverarrived_timestamp",
        "driverstarttheride_timestamp", "driverdone_timestamp", "clientcancel_timestamp",
        "drivercancel_timestamp", "order_modified_local", "cancel_before_accept_local",
    ):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")


def _parse_pass_dates(df: pd.DataFrame) -> None:
    for col in ("order_date_part", "user_reg_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")


def _parse_driver_dates(df: pd.DataFrame) -> None:
    for col in ("tender_date_part", "driver_reg_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")


def _truncate_and_load(
    engine,
    table: str,
    df: pd.DataFrame,
    chunk_size: int = 10_000,
) -> int:
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        conn.commit()
    n = len(df)
    if n == 0:
        return 0
    for i in tqdm(range(0, n, chunk_size), desc=f"load {table}"):
        chunk = df.iloc[i : i + chunk_size]
        chunk.to_sql(table, con=engine, if_exists="append", index=False)
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--incity", type=Path, default=ROOT / "incity.csv")
    ap.add_argument("--pass-detail", type=Path, dest="pass_csv", default=ROOT / "pass_detail.csv")
    ap.add_argument("--driver-detail", type=Path, dest="driver_csv", default=ROOT / "driver_detail.csv")
    ap.add_argument("--chunk", type=int, default=10_000)
    args = ap.parse_args()

    engine_root = create_engine("mysql+pymysql://root:@localhost:3306")
    with engine_root.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS drivee"))
        conn.commit()

    engine = create_engine("mysql+pymysql://root:@localhost:3306/drivee")
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("DROP TABLE IF EXISTS orders"))
        conn.execute(text(DDL_INCITY))
        conn.execute(text(DDL_PASS))
        conn.execute(text(DDL_DRIVER))
        conn.commit()
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()

    dfi = _read_csv(args.incity)
    _parse_incity_dates(dfi)
    dfp = _read_csv(args.pass_csv)
    _parse_pass_dates(dfp)
    dfd = _read_csv(args.driver_csv)
    _parse_driver_dates(dfd)

    n1 = _truncate_and_load(engine, "incity", dfi, args.chunk)
    n2 = _truncate_and_load(engine, "pass_detail", dfp, args.chunk)
    n3 = _truncate_and_load(engine, "driver_detail", dfd, args.chunk)
    print(f"Загружено: incity={n1}, pass_detail={n2}, driver_detail={n3} строк. Таблица orders удалена, если была.")

    print("Создаём индексы…")
    idx = [
        "CREATE INDEX idx_incity_city ON incity (city_id)",
        "CREATE INDEX idx_incity_order_ts ON incity (order_timestamp)",
        "CREATE INDEX idx_incity_status ON incity (status_order)",
        "CREATE INDEX idx_incity_status_time ON incity (status_order, order_timestamp)",
        "CREATE INDEX idx_incity_user ON incity (user_id)",
        "CREATE INDEX idx_incity_driver ON incity (driver_id)",
        "CREATE INDEX idx_pass_city_user_day ON pass_detail (city_id, user_id, order_date_part)",
        "CREATE INDEX idx_driver_city_drv_day ON driver_detail (city_id, driver_id, tender_date_part)",
    ]
    with engine.connect() as conn:
        for s in idx:
            try:
                conn.execute(text(s))
            except Exception as e:
                print(f"Предупреждение индекса: {e}")
        conn.commit()
    print("Готово.")


if __name__ == "__main__":
    main()
