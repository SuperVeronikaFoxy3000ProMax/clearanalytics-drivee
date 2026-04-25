# Умный Аналитик 

Веб-приложение для аналитики на естественном языке:

- frontend: `app.html` + `app.css`;
- backend: `FastAPI` (`api.py`);
- NL2SQL: rule-based логика + LLM (OpenAI-compatible endpoint, например LM Studio);
- БД терминов и отчетов: локальные `SQLite` файлы;
- отправка отчетов в Telegram через `telegram_bot.py`.

## Требования

- Python 3.11 (рекомендуется).
- MySQL/MariaDB с доступной базой `drivee`.
- Пользователь с правами `SELECT` для аналитических запросов.
- (Опционально) LLM endpoint OpenAI-compatible.
- (Опционально) Telegram bot token от [@BotFather](https://t.me/BotFather).

## Быстрый старт (локально)

### Windows (PowerShell)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

### Linux/macOS

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Запуск API:

```bash
uvicorn api:app --reload --host 127.0.0.1 --port 8000
```

Открыть UI: [http://127.0.0.1:8000/](http://127.0.0.1:8000/)

Запуск Telegram-бота (во втором терминале):

```bash
python telegram_bot.py
```

## Быстрый старт (Docker)

### 1) Подготовка `.env`

**Переменные окружения** — скопируйте `.env.example` в `.env` и заполните как минимум токен бота (если используете Telegram):

```text
TELEGRAM_BOT_TOKEN=ваш_токен
```

### 2) Запуск

```bash
docker compose up --build -d
```

UI: [http://127.0.0.1:8000/](http://127.0.0.1:8000/)

### 3) Логи

```bash
docker compose logs -f app
docker compose logs -f bot
```

### 4) Остановка

```bash
docker compose down
```

## Конфигурация

Проект читает настройки из переменных окружения:

- `DB_HOST`, `DB_PORT`, `DB_NAME`
- `RO_USER`, `RO_PASSWORD`
- `ADMIN_USER`, `ADMIN_PASSWORD`
- `QUERY_TIMEOUT_SEC`, `FORCED_LIMIT`
- `LLM_BASE_URL`, `LLM_MODEL`, `LLM_TIMEOUT_SEC`, `LLM_MAX_TOKENS`, `LLM_TEMPERATURE`
- `TELEGRAM_BOT_TOKEN`

Файл `project_env.py` загружает:

1. `.env`
2. `.env.telegram` - опиционально 

## Основные эндпоинты

- `GET /` — UI (`app.html`)
- `GET /health` — healthcheck API/БД
- `POST /ask` — обработка запроса пользователя и ответ с SQL/данными
- `GET /app.css` — стили

## Локальные базы проекта

При первом запуске создаются:

- `semantic_layer.db` — словарь терминов;
- `reports.db` — сохраненные отчеты, расписания, привязка Telegram ID/chat_id.

## Полезные скрипты

- `seed_db.py` — пересоздание начального словаря терминов.
- `download_db.py` — подготовка/загрузка данных в `drivee` (если разворачиваете с нуля).
- `seed_tuples.py` — заполнение тестовыми значениями.

## Типовые проблемы

- **UI не открывается**: проверьте, что `uvicorn` запущен на `127.0.0.1:8000`.
- **Ошибка подключения к БД**: проверьте `DB_HOST/PORT/NAME` и учетные данные.
- **LLM не отвечает**: проверьте `LLM_BASE_URL` и `LLM_MODEL`.
- **Telegram-бот не стартует**: проверьте `TELEGRAM_BOT_TOKEN`.
- **PNG в Telegram не формируется в Docker**: пересоберите контейнеры (`docker compose up --build -d`) после обновления `Dockerfile`.


