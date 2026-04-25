"""Загрузка переменных из файлов в корне проекта.

Порядок:
  1. `.env` — общие настройки (не коммитить).
  2. `.env.telegram` — опционально, только токен бота; перекрывает ключи из `.env`.
"""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent


def load_project_env() -> None:
    load_dotenv(_ROOT / ".env")
    load_dotenv(_ROOT / ".env.telegram", override=True)
