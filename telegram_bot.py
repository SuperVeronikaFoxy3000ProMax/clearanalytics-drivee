"""Telegram-бот: polling, /start → chat_id в reports.db. Токен: TELEGRAM_BOT_TOKEN в .env."""
from __future__ import annotations

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from project_env import load_project_env
from reports_db import init_db, save_chat_id, remove_chat_id

load_project_env()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = message.from_user.full_name or ""

    save_chat_id(
        telegram_user_id=user_id,
        chat_id=message.chat.id,
        username=username,
        full_name=full_name,
    )
    log.info("Registered chat_id=%s for user_id=%s (%s)", message.chat.id, user_id, username)

    await message.answer(
        f"👋 Привет, {full_name or 'друг'}!\n\n"
        f"Ваш Telegram ID: <code>{user_id}</code>\n\n"
        "Скопируйте это число и вставьте его в веб-интерфейс «Понятная Аналитика»: "
        "<b>Настройки (шестерёнка) → Куда отправлять отчёты → Telegram ID</b>, "
        "затем нажмите «Сохранить настройки» и при необходимости «Тест».\n\n"
        "Расписание рассылки отчётов в веб-интерфейсе задаётся по "
        "<b>московскому времени (МСК)</b> — как на сервере планировщика.\n\n"
        "Команда /id снова покажет это число.",
        parse_mode="HTML",
    )


async def cmd_id(message: Message) -> None:
    await message.answer(
        f"Ваш Telegram ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML",
    )


async def cmd_stop(message: Message) -> None:
    remove_chat_id(message.from_user.id)
    await message.answer("Вы отписались от рассылки. Чтобы подписаться снова — /start")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError(
            "Не задан TELEGRAM_BOT_TOKEN. "
            "Создайте `.env` или `.env.telegram` в корне проекта (см. `.env.example`)."
        )

    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_id, Command("id"))
    dp.message.register(cmd_stop, Command("stop"))

    log.info("Bot started. Polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановка бота (Ctrl+C)")
