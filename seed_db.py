"""Пересоздаёт semantic_layer.db с полным seed. Запускать отдельно."""
import sqlite3, json
from pathlib import Path

DB_PATH = Path(__file__).parent / "semantic_layer.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term TEXT NOT NULL UNIQUE COLLATE NOCASE,
    kind TEXT NOT NULL,
    column_expr TEXT NOT NULL,
    agg TEXT,
    filter_sql TEXT,
    synonyms_json TEXT NOT NULL DEFAULT '[]',
    is_user_added INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS term_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term_id INTEGER,
    term TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_json TEXT,
    actor TEXT DEFAULT 'system',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_terms_kind ON terms(kind);
"""

SEED = [
    # ID · ключи
    ("город (id)",            "id",        "city_id",                       None, None,                                                          ["город","city","регион"]),
    ("часовой пояс",          "id",        "offset_hours",                  None, None,                                                          ["UTC","timezone","пояс"]),
    ("заказ",                 "id",        "order_id",                      None, None,                                                          ["заказ","order","поездка"]),
    ("тендер",                "id",        "tender_id",                     None, None,                                                          ["тендер","подбор","аукцион"]),
    ("пользователь (клиент)", "id",        "user_id",                       None, None,                                                          ["клиент","user","пассажир"]),
    ("водитель (id)",         "id",        "driver_id",                     None, None,                                                          ["водитель","driver","исполнитель"]),
    # Статусы
    ("статус заказа",         "status",    "status_order",                  None, None,                                                          ["статус","отмена","выполнен","cancelled","completed"]),
    ("статус тендера",        "status",    "status_tender",                 None, None,                                                          ["подбор","тендер","auction"]),
    # Временные поля
    ("создание заказа",       "time",      "order_timestamp",               None, None,                                                          ["создан","заказан","created_at"]),
    ("начало тендера",        "time",      "tender_timestamp",              None, None,                                                          ["подбор","tender"]),
    ("принятие водителем",    "time",      "driveraccept_timestamp",        None, None,                                                          ["accept","назначен","взял заказ"]),
    ("прибытие водителя",     "time",      "driverarrived_timestamp",       None, None,                                                          ["подача","arrived","приехал"]),
    ("начало поездки",        "time",      "driverstarttheride_timestamp",  None, None,                                                          ["start","поехали","старт"]),
    ("завершение поездки",    "time",      "driverdone_timestamp",          None, None,                                                          ["done","completed","финиш","закончилась"]),
    ("отмена клиентом",       "time",      "clientcancel_timestamp",        None, None,                                                          ["клиент отменил","client cancel"]),
    ("отмена водителем",      "time",      "drivercancel_timestamp",        None, None,                                                          ["водитель отменил","driver cancel"]),
    ("последнее изменение",   "time",      "order_modified_local",          None, None,                                                          ["updated","изменён"]),
    ("отмена до принятия",    "time",      "cancel_before_accept_local",    None, None,                                                          ["pre-accept cancel","быстрая отмена"]),
    # Метрики поездки
    ("расстояние",            "trip",      "distance_in_meters",            None, None,                                                          ["дистанция","distance","км","метры"]),
    ("длительность",          "trip",      "duration_in_seconds",           None, None,                                                          ["время поездки","duration","секунды","минуты"]),
    ("стоимость заказа",      "trip",      "price_order_local",             None, None,                                                          ["цена","price","итог","стоимость"]),
    ("стоимость тендера",     "trip",      "price_tender_local",            None, None,                                                          ["тендерная цена","tender price"]),
    ("стартовая стоимость",   "trip",      "price_start_local",             None, None,                                                          ["начальная цена","start price"]),
    # Агрегаты и бизнес-метрики
    ("отмены",                "metric",    "status_order",                  "COUNT(*)",               "status_order = 'cancel'",                 ["отмена","отменен","canceled","cancelled","cancel"]),
    ("поездки",               "metric",    "status_order",                  "COUNT(*)",               "status_order = 'done'",                   ["поездка","заказы","ride","rides","order","orders","trips"]),
    ("выручка",               "metric",    "price_order_local",             "SUM(price_order_local)", "price_order_local IS NOT NULL AND status_order = 'done'", ["доход","revenue","оборот","sales","gmv"]),
    ("средняя цена",          "metric",    "price_order_local",             "AVG(price_order_local)", "price_order_local IS NOT NULL",            ["цена","price","средний чек","стоимость","avg price"]),
    ("город",                 "dimension", "city_id",                       None, None,                                                          ["города","city","cities","по городам"]),
    ("дата",                  "dimension", "DATE(order_timestamp)",         None, None,                                                          ["день","дни","date","по датам","по дням"]),
    ("месяц",                 "dimension", "MONTH(order_timestamp)",        None, None,                                                          ["месяц","month","по месяцам"]),
    ("водитель",              "dimension", "driver_id",                     None, None,                                                          ["водители","driver","drivers","топ водителей"]),
    ("статус",                "dimension", "status_order",                  None, None,                                                          ["статусы","status","by status"]),
]

def seed():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    inserted = 0
    for term, kind, col, agg, flt, syns in SEED:
        try:
            conn.execute(
                "INSERT INTO terms(term,kind,column_expr,agg,filter_sql,synonyms_json,is_user_added) VALUES(?,?,?,?,?,?,0)",
                (term, kind, col, agg, flt, json.dumps(syns, ensure_ascii=False))
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # уже есть
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    conn.close()
    print(f"Inserted {inserted} new terms. Total in DB: {total}")

if __name__ == "__main__":
    seed()
