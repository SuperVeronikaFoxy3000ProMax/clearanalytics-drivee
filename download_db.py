import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# создаём базу данных если её нет
engine_root = create_engine("mysql+pymysql://root:@localhost:3306")
with engine_root.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS drivee"))
    conn.commit()

# подключение к MySQL
engine = create_engine("mysql+pymysql://root:@localhost:3306/drivee")

# создаём таблицу если её нет
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS orders (
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
            distance_in_meters FLOAT,
            duration_in_seconds FLOAT,
            price_order_local FLOAT,
            price_tender_local FLOAT,
            price_start_local FLOAT
        )
    """))
    conn.commit()

# читаем CSV
df = pd.read_csv("train.csv")

# преобразуем даты
date_columns = [
    "order_timestamp",
    "tender_timestamp",
    "driveraccept_timestamp",
    "driverarrived_timestamp",
    "driverstarttheride_timestamp",
    "driverdone_timestamp",
    "clientcancel_timestamp",
    "drivercancel_timestamp",
    "order_modified_local",
    "cancel_before_accept_local"
]

for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# загрузка батчами (очень важно для больших данных)
chunk_size = 10000

for i in tqdm(range(0, len(df), chunk_size)):
    chunk = df.iloc[i:i+chunk_size]
    chunk.to_sql(
        "orders",
        con=engine,
        if_exists="append",
        index=False
    )

print("Загрузка завершена")

# создаём индексы для ускорения запросов
print("Создаём индексы...")
with engine.connect() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_city ON orders(city_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_order_time ON orders(order_timestamp)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_status ON orders(status_order)"))
    conn.commit()
print("Индексы созданы")