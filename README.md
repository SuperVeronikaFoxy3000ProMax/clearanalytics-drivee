# InstantData (a211)

Веб-интерфейс на **FastAPI** + статический фронт (`app.html` / `app.css`), NL→SQL через rule-based роутер и локальную LLM (**LM Studio**, OpenAI-совместимый API), семантический слой в **SQLite**, данные заказов в **MariaDB/MySQL**. Опционально: **Telegram-бот** и рассылка отчётов по расписанию.

## Что нужно заранее

- **Python** 3.10+ (рекомендуется 3.11).
- **MariaDB или MySQL** с базой `drivee` и таблицей `orders` (см. `config.py`: хост, порт, имя БД).
- Пользователь **read-only** для запросов аналитики (в `config.py` заданы `RO_USER` / `RO_PASSWORD`; при необходимости поправьте пароль и права `GRANT SELECT`).
- Для LLM-ветки: **LM Studio** (или аналог) с моделью, URL и имя модели — в `config.py` (`LLM_BASE_URL`, `LLM_MODEL`).
- Для Telegram: токен бота от [@BotFather](https://t.me/BotFather).

## Установка

Из корня репозитория:

```powershell
cd путь\к\clearanalytics-drivee
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

На Linux/macOS:

```bash
cd /path/to/clearanalytics-drivee
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Конфигурация

1. **`config.py`** — подключение к MySQL (`DB_HOST`, `DB_PORT`, `DB_NAME`, `RO_USER` / `RO_PASSWORD`, при необходимости `ADMIN_USER` / `ADMIN_PASSWORD` для валидации словаря по `INFORMATION_SCHEMA`), таймауты и параметры LM Studio.

2. **Переменные окружения** — скопируйте `.env.example` в `.env` и заполните как минимум токен бота (если используете Telegram):

   ```text
   TELEGRAM_BOT_TOKEN=ваш_токен
   ```


При первом запуске API создаются локальные файлы **`semantic_layer.db`** (словарь терминов) и **`reports.db`** (отчёты и привязки Telegram), если их ещё нет.

## Запуск через Docker (API + Telegram-бот)

### 1) Подготовьте `.env`

Скопируйте `.env.example` в `.env` и заполните:

```text
TELEGRAM_BOT_TOKEN=ваш_токен
```

### 2) Сборка и запуск

```bash
docker compose up --build -d
```

Откройте UI: **http://127.0.0.1:8000/**

### 3) Логи сервисов

```bash
docker compose logs -f app
docker compose logs -f bot
```

### 4) Остановка

```bash
docker compose down
```

## Запуск веб-приложения

```powershell
.\venv\Scripts\Activate.ps1
uvicorn api:app --reload --host 127.0.0.1 --port 8000
```

Откройте в браузере: **http://127.0.0.1:8000/**

## Запуск Telegram-бота (отдельный процесс)

Бот сохраняет `chat_id` в ту же **`reports.db`**, что и API (после `/start` в настройках веб-интерфейса можно указать Telegram ID для рассылки).

```powershell
.\venv\Scripts\Activate.ps1
python telegram_bot.py
```

Оставьте процесс запущенным (polling). API и бот могут работать параллельно в двух терминалах.

## Полезные пути

| Путь | Назначение |
|------|------------|
| `GET /health` | Проверка доступности API и БД |
| `/` | Главная страница (`app.html`) |
| `/app.css` | Стили |

## Дополнительно (по необходимости)

- **`seed_db.py`** — пересоздать `semantic_layer.db` с полным начальным набором терминов (запуск вручную, когда нужен «чистый» словарь).
- **`download_db.py`** — утилита для поднятия схемы `drivee` / `orders` и загрузки данных из CSV (если вы разворачиваете БД с нуля).

## Устранение неполадок

- **Бэкенд недоступен в UI** — убедитесь, что `uvicorn` запущен и страница открыта с того же хоста/порта, что и API (по умолчанию `127.0.0.1:8000`).
- **Ошибки MySQL** — проверьте `config.py`, что сервер запущен, база `drivee` существует, у read-only пользователя есть `SELECT` на нужные таблицы.
- **LLM не отвечает** — проверьте, что LM Studio слушает `LLM_BASE_URL` и загружена модель с именем, совпадающим с `LLM_MODEL`.
