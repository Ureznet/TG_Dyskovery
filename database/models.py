# -*- coding: utf-8 -*-
"""
Dataclass-модели сущностей базы данных (ТЗ v2.0, раздел 6).

Соответствуют таблицам SQLite: accounts, proxies, channels, tasks,
task_results, checker_results, settings.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Account:
    """Аккаунт Telegram (id, phone, username, session_data, proxy_id,
    status, sleep_until, last_used, created_at)."""

    id: Optional[int] = field(default=None)
    phone: Optional[str] = field(default=None)
    username: Optional[str] = field(default=None)
    session_data: Optional[str] = field(default=None)  # зашифрованная сессия
    proxy_id: Optional[int] = field(default=None)
    status: Optional[str] = field(default="active")  # active/banned/flood/sleep/error
    sleep_until: Optional[str] = field(default=None)  # ISO datetime
    last_used: Optional[str] = field(default=None)    # ISO datetime
    created_at: Optional[str] = field(default=None)   # ISO datetime


@dataclass
class Proxy:
    """Прокси-сервер (id, name, type, host, port, username, password)."""

    id: Optional[int] = field(default=None)
    name: Optional[str] = field(default=None)
    type: Optional[str] = field(default="socks5")  # socks4/socks5/http
    host: Optional[str] = field(default=None)
    port: Optional[int] = field(default=None)
    username: Optional[str] = field(default=None)
    password: Optional[str] = field(default=None)  # зашифрованный пароль


@dataclass
class Channel:
    """Telegram-канал (tg_id, username, title, description, type,
    access_type, participants_count, last_post_date, last_comment_date,
    is_active, source, updated_at)."""

    tg_id: Optional[int] = field(default=None)  # PRIMARY KEY
    username: Optional[str] = field(default=None)
    title: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)
    type: Optional[str] = field(default="channel")  # channel/group/supergroup
    access_type: Optional[str] = field(default="public")  # public/private
    participants_count: Optional[int] = field(default=0)
    last_post_date: Optional[str] = field(default=None)      # ISO datetime
    last_comment_date: Optional[str] = field(default=None)   # ISO datetime
    is_active: Optional[int] = field(default=0)              # 1/0
    source: Optional[str] = field(default=None)              # источник discovery
    updated_at: Optional[str] = field(default=None)          # ISO datetime


@dataclass
class Task:
    """Задача (id, name, type, status, config, created_at, updated_at)."""

    id: Optional[int] = field(default=None)
    name: Optional[str] = field(default=None)
    type: Optional[str] = field(default=None)     # discovery/checker/export/parser
    status: Optional[str] = field(default="pending")  # pending/running/paused/done/error
    config: Optional[str] = field(default=None)   # JSON-конфиг задачи
    created_at: Optional[str] = field(default=None)
    updated_at: Optional[str] = field(default=None)


@dataclass
class TaskResult:
    """Результат выполнения задачи по каналу (составной PK: task_id + channel_tg_id)."""

    task_id: Optional[int] = field(default=None)      # часть PK
    channel_tg_id: Optional[int] = field(default=None)  # часть PK
    status: Optional[str] = field(default=None)       # ok/error/skipped
    error_message: Optional[str] = field(default=None)


@dataclass
class CheckerResult:
    """Результат проверки канала чекером."""

    id: Optional[int] = field(default=None)
    username: Optional[str] = field(default=None)
    tg_id: Optional[int] = field(default=None)
    title: Optional[str] = field(default=None)
    access_type: Optional[str] = field(default=None)      # public/private
    participants_count: Optional[int] = field(default=0)
    last_post_date: Optional[str] = field(default=None)
    last_comment_date: Optional[str] = field(default=None)
    is_active: Optional[int] = field(default=0)           # 1 — активен по порогу
    checked_at: Optional[str] = field(default=None)


@dataclass
class Setting:
    """Настройка вида ключ-значение."""

    key: Optional[str] = field(default=None)      # PRIMARY KEY
    value: Optional[str] = field(default=None)
    updated_at: Optional[str] = field(default=None)
