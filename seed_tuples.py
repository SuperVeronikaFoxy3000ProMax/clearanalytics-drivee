"""Канонический seed семантического слоя: список кортежей
(term, kind, column_expr, agg, filter_sql, synonyms).

Сюда вносятся правки; semantic_layer._SEED и seed_db.py собираются из этого списка,
чтобы не дублировать и не разъезжались копии."""

from __future__ import annotations

# kind: id | status | time | trip | metric | dimension

SEED: list[
    tuple[
        str,  # term
        str,  # kind
        str,  # column_expr
        str | None,  # agg
        str | None,  # filter_sql
        list[str],  # synonyms
    ]
] = [
    # ——— ID · ключи
    ("город (id)", "id", "city_id", None, None, ["город", "city", "регион"]),
    ("часовой пояс", "id", "offset_hours", None, None, ["UTC", "timezone", "пояс"]),
    ("заказ", "id", "order_id", None, None, ["заказ", "order", "поездка"]),
    ("тендер", "id", "tender_id", None, None, ["тендер", "подбор", "аукцион"]),
    ("пользователь (клиент)", "id", "user_id", None, None, ["клиент", "user", "пассажир"]),
    ("водитель (id)", "id", "driver_id", None, None, ["водители", "driver", "drivers", "топ водителей", "исполнитель"]),
    # ——— Статусы
    ("статус заказа", "status", "status_order", None, None, ["статус", "отмена", "выполнен", "cancelled", "completed"]),
    ("статус тендера", "status", "status_tender", None, None, ["подбор", "тендер", "auction"]),
    # ——— Временные поля
    ("создание заказа", "time", "order_timestamp", None, None, ["создан", "заказан", "created_at"]),
    ("начало тендера", "time", "tender_timestamp", None, None, ["подбор", "tender"]),
    ("принятие водителем", "time", "driveraccept_timestamp", None, None, ["accept", "назначен", "взял заказ"]),
    ("прибытие водителя", "time", "driverarrived_timestamp", None, None, ["подача", "arrived", "приехал"]),
    ("начало поездки", "time", "driverstarttheride_timestamp", None, None, ["start", "поехали", "старт"]),
    ("завершение поездки", "time", "driverdone_timestamp", None, None, ["done", "completed", "финиш", "закончилась"]),
    ("отмена клиентом", "time", "clientcancel_timestamp", None, None, ["клиент отменил", "client cancel"]),
    ("отмена водителем", "time", "drivercancel_timestamp", None, None, ["водитель отменил", "driver cancel"]),
    ("последнее изменение", "time", "order_modified_local", None, None, ["updated", "изменён"]),
    ("отмена до принятия", "time", "cancel_before_accept_local", None, None, ["pre-accept cancel", "быстрая отмена"]),
    # ——— Метрики поездки
    ("расстояние", "trip", "distance_in_meters", None, None, ["дистанция", "distance", "км", "метры"]),
    ("длительность", "trip", "duration_in_seconds", None, None, ["время поездки", "duration", "секунды", "минуты"]),
    ("стоимость заказа", "trip", "price_order_local", None, None, ["цена", "price", "итог", "стоимость"]),
    ("стоимость тендера", "trip", "price_tender_local", None, None, ["тендерная цена", "tender price"]),
    ("стартовая стоимость", "trip", "price_start_local", None, None, ["начальная цена", "start price"]),
    # ——— Агрегаты и бизнес-метрики (витрина incity)
    (
        "отмены", "metric", "status_order", "COUNT(*)", "status_order = 'cancel'",
        ["отмена", "отменен", "canceled", "cancelled", "cancel"],
    ),
    (
        "поездки", "metric", "status_order", "COUNT(*)", "status_order = 'done'",
        ["поездка", "заказы", "ride", "rides", "order", "orders", "trips"],
    ),
    (
        "выручка", "metric", "price_order_local", "SUM(price_order_local)",
        "price_order_local IS NOT NULL AND status_order = 'done'",
        ["доход", "revenue", "оборот", "sales", "gmv"],
    ),
    (
        "средняя цена", "metric", "price_order_local", "AVG(price_order_local)",
        "price_order_local IS NOT NULL",
        ["цена", "price", "средний чек", "стоимость", "avg price"],
    ),
    ("город", "dimension", "city_id", None, None, ["города", "city", "cities", "по городам"]),
    ("дата", "dimension", "DATE(order_timestamp)", None, None, ["день", "дни", "date", "по датам", "по дням"]),
    ("месяц", "dimension", "MONTH(order_timestamp)", None, None, ["месяц", "month", "по месяцам"]),
    ("водитель", "dimension", "driver_id", None, None, ["водители", "водителей", "driver", "drivers", "топ водителей"]),
    ("статус", "dimension", "status_order", None, None, ["статусы", "status", "by status"]),

    # pass_detail — дневные метрики пассажиров
    (
        "дата (пассажиры, витрина)", "dimension", "pass_detail.order_date_part", None, None,
        ["дневные пассажиры", "order_date_part", "метрики пассажира по дням"],
    ),
    ("регистрация пассажира", "time", "pass_detail.user_reg_date", None, None, ["user_reg_date", "дата регистрации пассажира"]),
    (
        "заказы пассажира в день (сумма)", "metric", "pass_detail.orders_count", "SUM(pass_detail.orders_count)", None,
        ["сумма orders_count", "дневные заказы пассажиров"],
    ),
    (
        "заказы пассажира с тендерами (сумма)", "metric", "pass_detail.orders_cnt_with_tenders",
        "SUM(pass_detail.orders_cnt_with_tenders)", None, ["тендеры пассажир", "orders_cnt_with_tenders"],
    ),
    (
        "принятые заказы пассажира (сумма)", "metric", "pass_detail.orders_cnt_accepted",
        "SUM(pass_detail.orders_cnt_accepted)", None, ["принятые пассажир", "orders_cnt_accepted"],
    ),
    (
        "поездки пассажира в день (сумма)", "metric", "pass_detail.rides_count", "SUM(pass_detail.rides_count)", None,
        ["rides_count пассажир", "суммарно поездок пассажир"],
    ),
    (
        "отмены пассажира после принятия (сумма)", "metric", "pass_detail.client_cancel_after_accept",
        "SUM(pass_detail.client_cancel_after_accept)", None, ["client_cancel_after_accept"],
    ),
    (
        "длительность поездок пассажира (сек, сумма)", "metric", "pass_detail.rides_time_sum_seconds",
        "SUM(pass_detail.rides_time_sum_seconds)", None, ["rides_time_sum_seconds"],
    ),
    (
        "время онлайн пассажира (сек, сумма)", "metric", "pass_detail.online_time_sum_seconds",
        "SUM(pass_detail.online_time_sum_seconds)", None, ["online_time пассажир"],
    ),

    # driver_detail — дневные метрики водителей
    (
        "дата (водители, витрина)", "dimension", "driver_detail.tender_date_part", None, None,
        ["дневные водители", "tender_date_part", "метрики водителя по дням"],
    ),
    (
        "регистрация водителя", "time", "driver_detail.driver_reg_date", None, None,
        ["driver_reg_date", "дата регистрации водителя"],
    ),
    (
        "заказы водителя в день (сумма)", "metric", "driver_detail.orders", "SUM(driver_detail.orders)", None,
        ["дневной счётчик orders водителя", "сумма orders водителя"],
    ),
    (
        "заказы водителя с тендерами (сумма)", "metric", "driver_detail.orders_cnt_with_tenders",
        "SUM(driver_detail.orders_cnt_with_tenders)", None, [],
    ),
    (
        "принятые заказы водителя (сумма)", "metric", "driver_detail.orders_cnt_accepted",
        "SUM(driver_detail.orders_cnt_accepted)", None, [],
    ),
    (
        "поездки водителя в день (сумма)", "metric", "driver_detail.rides_count", "SUM(driver_detail.rides_count)", None,
        ["rides_count водителя", "топ поездок водителя витрина"],
    ),
    (
        "отмены пассажиром после принятия (водитель, сумма)", "metric", "driver_detail.client_cancel_after_accept",
        "SUM(driver_detail.client_cancel_after_accept)", None, [],
    ),
    (
        "длительность поездок водителя (сек, сумма)", "metric", "driver_detail.rides_time_sum_seconds",
        "SUM(driver_detail.rides_time_sum_seconds)", None, [],
    ),
    (
        "время онлайн водителя (сек, сумма)", "metric", "driver_detail.online_time_sum_seconds",
        "SUM(driver_detail.online_time_sum_seconds)", None, [],
    ),
]
