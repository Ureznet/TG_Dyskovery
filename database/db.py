# -*- coding: utf-8 -*-
"""
Асинхронный менеджер базы данных SQLite (aiosqlite).

Содержит схему БД из ТЗ v2.0 (раздел 6), включая FTS5 виртуальную таблицу
для полнотекстового поиска по каналам, и CRUD-методы для всех сущностей.
"""

import os
import shutil
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite
from loguru import logger

from database.models import (
    Account,
    Channel,
    CheckerResult,
    Proxy,
    Setting,
    Task,
    TaskResult,
)

# SQL-схема базы данных согласно ТЗ
_SCHEMA: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT UNIQUE,
        username TEXT,
        session_data TEXT,
        proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
        status TEXT NOT NULL DEFAULT 'active',
        sleep_until TEXT,
        last_used TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS proxies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        type TEXT NOT NULL DEFAULT 'socks5',
        host TEXT NOT NULL,
        port INTEGER NOT NULL,
        username TEXT,
        password TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS channels (
        tg_id INTEGER PRIMARY KEY,
        username TEXT,
        title TEXT,
        description TEXT,
        type TEXT NOT NULL DEFAULT 'channel',
        access_type TEXT NOT NULL DEFAULT 'public',
        participants_count INTEGER NOT NULL DEFAULT 0,
        last_post_date TEXT,
        last_comment_date TEXT,
        is_active INTEGER NOT NULL DEFAULT 0,
        source TEXT,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        config TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS task_results (
        task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        channel_tg_id INTEGER NOT NULL,
        status TEXT,
        error_message TEXT,
        PRIMARY KEY (task_id, channel_tg_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS checker_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        tg_id INTEGER,
        title TEXT,
        access_type TEXT,
        participants_count INTEGER NOT NULL DEFAULT 0,
        last_post_date TEXT,
        last_comment_date TEXT,
        is_active INTEGER NOT NULL DEFAULT 0,
        checked_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    # Индексы для часто используемых выборок
    "CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)",
    "CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username)",
    "CREATE INDEX IF NOT EXISTS idx_channels_is_active ON channels(is_active)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)",
    "CREATE INDEX IF NOT EXISTS idx_checker_results_username ON checker_results(username)",
    # FTS5 виртуальная таблица полнотекстового поиска по каналам
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS channels_fts USING fts5(
        title,
        description,
        username,
        content='channels',
        content_rowid='tg_id'
    )
    """,
    # Триггеры синхронизации FTS-индекса с таблицей channels
    """
    CREATE TRIGGER IF NOT EXISTS channels_ai AFTER INSERT ON channels BEGIN
        INSERT INTO channels_fts(rowid, title, description, username)
        VALUES (new.tg_id, new.title, new.description, new.username);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS channels_ad AFTER DELETE ON channels BEGIN
        INSERT INTO channels_fts(channels_fts, rowid, title, description, username)
        VALUES ('delete', old.tg_id, old.title, old.description, old.username);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS channels_au AFTER UPDATE ON channels BEGIN
        INSERT INTO channels_fts(channels_fts, rowid, title, description, username)
        VALUES ('delete', old.tg_id, old.title, old.description, old.username);
        INSERT INTO channels_fts(rowid, title, description, username)
        VALUES (new.tg_id, new.title, new.description, new.username);
    END
    """,
]


def _utcnow_iso() -> str:
    """Текущее время в UTC, ISO-формат."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class DatabaseManager:
    """Асинхронный менеджер БД SQLite на базе aiosqlite."""

    def __init__(self, db_path: str = "data/telegram_discovery.db") -> None:
        """
        :param db_path: Путь к файлу базы данных.
        """
        self.db_path: str = db_path
        self.db: Optional[aiosqlite.Connection] = None

    # ------------------------------------------------------------------
    # Инициализация / закрытие
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        """Открытие соединения и создание всех таблиц схемы."""
        try:
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            self.db = await aiosqlite.connect(self.db_path)
            self.db.row_factory = aiosqlite.Row
            # Включаемsupport внешних ключей и WAL для конкурентности
            await self.db.execute("PRAGMA foreign_keys = ON")
            await self.db.execute("PRAGMA journal_mode = WAL")

            for statement in _SCHEMA:
                await self.db.execute(statement)
            await self.db.commit()
            logger.info(f"База данных инициализирована: {self.db_path}")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка инициализации базы данных: {exc}")
            raise

    async def close(self) -> None:
        """Закрытие соединения с БД."""
        try:
            if self.db is not None:
                await self.db.close()
                self.db = None
                logger.info("Соединение с базой данных закрыто")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка закрытия соединения с БД: {exc}")

    def _check_conn(self) -> aiosqlite.Connection:
        """Проверка, что соединение открыто."""
        if self.db is None:
            raise RuntimeError("База данных не инициализирована, вызовите initialize()")
        return self.db

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------
    async def create_account(self, account: Account) -> int:
        """Создание аккаунта. Возвращает id новой записи."""
        conn = self._check_conn()
        try:
            cursor = await conn.execute(
                """
                INSERT INTO accounts (phone, username, session_data, proxy_id,
                                      status, sleep_until, last_used, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account.phone,
                    account.username,
                    account.session_data,
                    account.proxy_id,
                    account.status or "active",
                    account.sleep_until,
                    account.last_used,
                    account.created_at or _utcnow_iso(),
                ),
            )
            await conn.commit()
            logger.debug(f"Создан аккаунт id={cursor.lastrowid}")
            return int(cursor.lastrowid or 0)
        except sqlite3.IntegrityError as exc:
            logger.error(f"Ошибка создания аккаунта (дубликат): {exc}")
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка создания аккаунта: {exc}")
            raise

    async def get_account(self, account_id: int) -> Optional[Account]:
        """Получение аккаунта по id."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_account(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения аккаунта id={account_id}: {exc}")
            raise

    async def update_account(self, account: Account) -> bool:
        """Обновление существующего аккаунта (по id)."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE accounts SET phone = ?, username = ?, session_data = ?,
                       proxy_id = ?, status = ?, sleep_until = ?, last_used = ?
                WHERE id = ?
                """,
                (
                    account.phone,
                    account.username,
                    account.session_data,
                    account.proxy_id,
                    account.status,
                    account.sleep_until,
                    account.last_used,
                    account.id,
                ),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления аккаунта id={account.id}: {exc}")
            raise

    async def delete_account(self, account_id: int) -> bool:
        """Удаление аккаунта по id."""
        conn = self._check_conn()
        try:
            await conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления аккаунта id={account_id}: {exc}")
            raise

    async def get_all_accounts(self) -> list[Account]:
        """Список всех аккаунтов."""
        conn = self._check_conn()
        try:
            accounts: list[Account] = []
            async with conn.execute("SELECT * FROM accounts ORDER BY id") as cursor:
                async for row in cursor:
                    accounts.append(self._row_to_account(row))
            return accounts
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения списка аккаунтов: {exc}")
            raise

    @staticmethod
    def _row_to_account(row: aiosqlite.Row) -> Account:
        """Преобразование строки результата в dataclass Account."""
        return Account(
            id=row["id"],
            phone=row["phone"],
            username=row["username"],
            session_data=row["session_data"],
            proxy_id=row["proxy_id"],
            status=row["status"],
            sleep_until=row["sleep_until"],
            last_used=row["last_used"],
            created_at=row["created_at"],
        )

    # ------------------------------------------------------------------
    # Proxies
    # ------------------------------------------------------------------
    async def create_proxy(self, proxy: Proxy) -> int:
        """Создание прокси. Возвращает id новой записи."""
        conn = self._check_conn()
        try:
            cursor = await conn.execute(
                """
                INSERT INTO proxies (name, type, host, port, username, password)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (proxy.name, proxy.type or "socks5", proxy.host,
                 proxy.port, proxy.username, proxy.password),
            )
            await conn.commit()
            logger.debug(f"Создан прокси id={cursor.lastrowid}")
            return int(cursor.lastrowid or 0)
        except sqlite3.IntegrityError as exc:
            logger.error(f"Ошибка создания прокси (дубликат): {exc}")
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка создания прокси: {exc}")
            raise

    async def get_proxy(self, proxy_id: int) -> Optional[Proxy]:
        """Получение прокси по id."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM proxies WHERE id = ?", (proxy_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_proxy(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения прокси id={proxy_id}: {exc}")
            raise

    async def update_proxy(self, proxy: Proxy) -> bool:
        """Обновление прокси по id."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE proxies SET name = ?, type = ?, host = ?,
                       port = ?, username = ?, password = ?
                WHERE id = ?
                """,
                (proxy.name, proxy.type, proxy.host,
                 proxy.port, proxy.username, proxy.password, proxy.id),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления прокси id={proxy.id}: {exc}")
            raise

    async def delete_proxy(self, proxy_id: int) -> bool:
        """Удаление прокси по id."""
        conn = self._check_conn()
        try:
            await conn.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления прокси id={proxy_id}: {exc}")
            raise

    async def get_all_proxies(self) -> list[Proxy]:
        """Список всех прокси."""
        conn = self._check_conn()
        try:
            proxies: list[Proxy] = []
            async with conn.execute("SELECT * FROM proxies ORDER BY id") as cursor:
                async for row in cursor:
                    proxies.append(self._row_to_proxy(row))
            return proxies
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения списка прокси: {exc}")
            raise

    @staticmethod
    def _row_to_proxy(row: aiosqlite.Row) -> Proxy:
        """Преобразование строки результата в dataclass Proxy."""
        return Proxy(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            host=row["host"],
            port=row["port"],
            username=row["username"],
            password=row["password"],
        )

    # ------------------------------------------------------------------
    # Channels
    # ------------------------------------------------------------------
    async def create_channel(self, channel: Channel) -> int:
        """Создание или обновление канала (UPSERT по tg_id)."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                INSERT INTO channels (tg_id, username, title, description, type,
                        access_type, participants_count, last_post_date,
                        last_comment_date, is_active, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_id) DO UPDATE SET
                    username = excluded.username,
                    title = excluded.title,
                    description = excluded.description,
                    type = excluded.type,
                    access_type = excluded.access_type,
                    participants_count = excluded.participants_count,
                    last_post_date = excluded.last_post_date,
                    last_comment_date = excluded.last_comment_date,
                    is_active = excluded.is_active,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                (
                    channel.tg_id,
                    channel.username,
                    channel.title,
                    channel.description,
                    channel.type or "channel",
                    channel.access_type or "public",
                    channel.participants_count or 0,
                    channel.last_post_date,
                    channel.last_comment_date,
                    channel.is_active or 0,
                    channel.source,
                    channel.updated_at or _utcnow_iso(),
                ),
            )
            await conn.commit()
            logger.debug(f"Канал tg_id={channel.tg_id} сохранён")
            return int(channel.tg_id or 0)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка сохранения канала tg_id={channel.tg_id}: {exc}")
            raise

    async def get_channel(self, tg_id: int) -> Optional[Channel]:
        """Получение канала по tg_id."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM channels WHERE tg_id = ?", (tg_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_channel(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения канала tg_id={tg_id}: {exc}")
            raise

    async def update_channel(self, channel: Channel) -> bool:
        """Обновление канала по tg_id."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE channels SET username = ?, title = ?, description = ?,
                       type = ?, access_type = ?, participants_count = ?,
                       last_post_date = ?, last_comment_date = ?, is_active = ?,
                       source = ?, updated_at = ?
                WHERE tg_id = ?
                """,
                (
                    channel.username, channel.title, channel.description,
                    channel.type, channel.access_type, channel.participants_count,
                    channel.last_post_date, channel.last_comment_date,
                    channel.is_active, channel.source,
                    channel.updated_at or _utcnow_iso(), channel.tg_id,
                ),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления канала tg_id={channel.tg_id}: {exc}")
            raise

    async def delete_channel(self, tg_id: int) -> bool:
        """Удаление канала по tg_id."""
        conn = self._check_conn()
        try:
            await conn.execute("DELETE FROM channels WHERE tg_id = ?", (tg_id,))
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления канала tg_id={tg_id}: {exc}")
            raise

    async def get_all_channels(self) -> list[Channel]:
        """Список всех каналов."""
        conn = self._check_conn()
        try:
            channels: list[Channel] = []
            async with conn.execute("SELECT * FROM channels ORDER BY tg_id") as cursor:
                async for row in cursor:
                    channels.append(self._row_to_channel(row))
            return channels
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения списка каналов: {exc}")
            raise

    async def search_channels_fts(self, query: str, limit: int = 100) -> list[Channel]:
        """
        Полнотекстовый поиск каналов через FTS5.

        :param query: Поисковый запрос (MATCH-синтаксис FTS5).
        :param limit: Максимальное количество результатов.
        """
        conn = self._check_conn()
        try:
            channels: list[Channel] = []
            async with conn.execute(
                """
                SELECT c.* FROM channels c
                JOIN channels_fts f ON c.tg_id = f.rowid
                WHERE channels_fts MATCH ?
                ORDER BY bm25(channels_fts)
                LIMIT ?
                """,
                (query, limit),
            ) as cursor:
                async for row in cursor:
                    channels.append(self._row_to_channel(row))
            return channels
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка FTS-поиска каналов по запросу '{query}': {exc}")
            raise

    @staticmethod
    def _row_to_channel(row: aiosqlite.Row) -> Channel:
        """Преобразование строки результата в dataclass Channel."""
        return Channel(
            tg_id=row["tg_id"],
            username=row["username"],
            title=row["title"],
            description=row["description"],
            type=row["type"],
            access_type=row["access_type"],
            participants_count=row["participants_count"],
            last_post_date=row["last_post_date"],
            last_comment_date=row["last_comment_date"],
            is_active=row["is_active"],
            source=row["source"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------
    async def create_task(self, task: Task) -> int:
        """Создание задачи. Возвращает id новой записи."""
        conn = self._check_conn()
        try:
            now = _utcnow_iso()
            cursor = await conn.execute(
                """
                INSERT INTO tasks (name, type, status, config, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (task.name, task.type, task.status or "pending",
                 task.config, now, now),
            )
            await conn.commit()
            logger.debug(f"Создана задача id={cursor.lastrowid}")
            return int(cursor.lastrowid or 0)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка создания задачи: {exc}")
            raise

    async def get_task(self, task_id: int) -> Optional[Task]:
        """Получение задачи по id."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_task(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения задачи id={task_id}: {exc}")
            raise

    async def update_task(self, task: Task) -> bool:
        """Обновление задачи по id."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE tasks SET name = ?, type = ?, status = ?,
                       config = ?, updated_at = ?
                WHERE id = ?
                """,
                (task.name, task.type, task.status, task.config,
                 _utcnow_iso(), task.id),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления задачи id={task.id}: {exc}")
            raise

    async def delete_task(self, task_id: int) -> bool:
        """Удаление задачи по id (результаты каскадно)."""
        conn = self._check_conn()
        try:
            await conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления задачи id={task_id}: {exc}")
            raise

    async def get_all_tasks(self) -> list[Task]:
        """Список всех задач."""
        conn = self._check_conn()
        try:
            tasks: list[Task] = []
            async with conn.execute("SELECT * FROM tasks ORDER BY id") as cursor:
                async for row in cursor:
                    tasks.append(self._row_to_task(row))
            return tasks
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения списка задач: {exc}")
            raise

    @staticmethod
    def _row_to_task(row: aiosqlite.Row) -> Task:
        """Преобразование строки результата в dataclass Task."""
        return Task(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            status=row["status"],
            config=row["config"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # TaskResults
    # ------------------------------------------------------------------
    async def create_task_result(self, result: TaskResult) -> bool:
        """Создание (или обновление) результата задачи по каналу."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                INSERT INTO task_results (task_id, channel_tg_id, status, error_message)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(task_id, channel_tg_id) DO UPDATE SET
                    status = excluded.status,
                    error_message = excluded.error_message
                """,
                (result.task_id, result.channel_tg_id,
                 result.status, result.error_message),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f"Ошибка сохранения результата задачи "
                f"(task={result.task_id}, channel={result.channel_tg_id}): {exc}"
            )
            raise

    async def get_task_result(self, task_id: int, channel_tg_id: int) -> Optional[TaskResult]:
        """Получение результата задачи по составному PK."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM task_results WHERE task_id = ? AND channel_tg_id = ?",
                (task_id, channel_tg_id),
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_task_result(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения результата задачи task={task_id}: {exc}")
            raise

    async def update_task_result(self, result: TaskResult) -> bool:
        """Обновление результата задачи."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE task_results SET status = ?, error_message = ?
                WHERE task_id = ? AND channel_tg_id = ?
                """,
                (result.status, result.error_message,
                 result.task_id, result.channel_tg_id),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления результата задачи task={result.task_id}: {exc}")
            raise

    async def delete_task_result(self, task_id: int, channel_tg_id: int) -> bool:
        """Удаление результата задачи."""
        conn = self._check_conn()
        try:
            await conn.execute(
                "DELETE FROM task_results WHERE task_id = ? AND channel_tg_id = ?",
                (task_id, channel_tg_id),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления результата задачи task={task_id}: {exc}")
            raise

    async def get_task_results(self, task_id: int) -> list[TaskResult]:
        """Список результатов конкретной задачи."""
        conn = self._check_conn()
        try:
            results: list[TaskResult] = []
            async with conn.execute(
                "SELECT * FROM task_results WHERE task_id = ? ORDER BY channel_tg_id",
                (task_id,),
            ) as cursor:
                async for row in cursor:
                    results.append(self._row_to_task_result(row))
            return results
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения результатов задачи task={task_id}: {exc}")
            raise

    @staticmethod
    def _row_to_task_result(row: aiosqlite.Row) -> TaskResult:
        """Преобразование строки результата в dataclass TaskResult."""
        return TaskResult(
            task_id=row["task_id"],
            channel_tg_id=row["channel_tg_id"],
            status=row["status"],
            error_message=row["error_message"],
        )

    # ------------------------------------------------------------------
    # CheckerResults
    # ------------------------------------------------------------------
    async def create_checker_result(self, result: CheckerResult) -> int:
        """Сохранение результата проверки чекера. Возвращает id записи."""
        conn = self._check_conn()
        try:
            cursor = await conn.execute(
                """
                INSERT INTO checker_results (username, tg_id, title, access_type,
                        participants_count, last_post_date, last_comment_date,
                        is_active, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.username, result.tg_id, result.title,
                    result.access_type, result.participants_count or 0,
                    result.last_post_date, result.last_comment_date,
                    result.is_active or 0, result.checked_at or _utcnow_iso(),
                ),
            )
            await conn.commit()
            return int(cursor.lastrowid or 0)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка сохранения результата чекера: {exc}")
            raise

    async def get_checker_result(self, result_id: int) -> Optional[CheckerResult]:
        """Получение результата чекера по id."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM checker_results WHERE id = ?", (result_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return self._row_to_checker_result(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения результата чекера id={result_id}: {exc}")
            raise

    async def update_checker_result(self, result: CheckerResult) -> bool:
        """Обновление результата чекера по id."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                UPDATE checker_results SET username = ?, tg_id = ?, title = ?,
                       access_type = ?, participants_count = ?, last_post_date = ?,
                       last_comment_date = ?, is_active = ?, checked_at = ?
                WHERE id = ?
                """,
                (
                    result.username, result.tg_id, result.title,
                    result.access_type, result.participants_count,
                    result.last_post_date, result.last_comment_date,
                    result.is_active, result.checked_at, result.id,
                ),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления результата чекера id={result.id}: {exc}")
            raise

    async def delete_checker_result(self, result_id: int) -> bool:
        """Удаление результата чекера по id."""
        conn = self._check_conn()
        try:
            await conn.execute(
                "DELETE FROM checker_results WHERE id = ?", (result_id,)
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка удаления результата чекера id={result_id}: {exc}")
            raise

    async def get_all_checker_results(self) -> list[CheckerResult]:
        """Список всех результатов чекера."""
        conn = self._check_conn()
        try:
            results: list[CheckerResult] = []
            async with conn.execute(
                "SELECT * FROM checker_results ORDER BY id DESC"
            ) as cursor:
                async for row in cursor:
                    results.append(self._row_to_checker_result(row))
            return results
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения списка результатов чекера: {exc}")
            raise

    @staticmethod
    def _row_to_checker_result(row: aiosqlite.Row) -> CheckerResult:
        """Преобразование строки результата в dataclass CheckerResult."""
        return CheckerResult(
            id=row["id"],
            username=row["username"],
            tg_id=row["tg_id"],
            title=row["title"],
            access_type=row["access_type"],
            participants_count=row["participants_count"],
            last_post_date=row["last_post_date"],
            last_comment_date=row["last_comment_date"],
            is_active=row["is_active"],
            checked_at=row["checked_at"],
        )

    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------
    async def get_settings(self) -> dict[str, str]:
        """Возвращает все настройки как словарь key -> value."""
        conn = self._check_conn()
        try:
            settings: dict[str, str] = {}
            async with conn.execute("SELECT key, value FROM settings") as cursor:
                async for row in cursor:
                    settings[row["key"]] = row["value"]
            return settings
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения настроек: {exc}")
            raise

    async def get_setting(self, key: str) -> Optional[str]:
        """Получение значения одной настройки по ключу."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT value FROM settings WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row["value"] if row else None
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения настройки '{key}': {exc}")
            raise

    async def update_setting(self, key: str, value: Any) -> bool:
        """Создание или обновление настройки (UPSERT)."""
        conn = self._check_conn()
        try:
            await conn.execute(
                """
                INSERT INTO settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, str(value), _utcnow_iso()),
            )
            await conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка обновления настройки '{key}': {exc}")
            raise

    async def get_setting_object(self, key: str) -> Optional[Setting]:
        """Получение настройки в виде dataclass Setting."""
        conn = self._check_conn()
        try:
            async with conn.execute(
                "SELECT * FROM settings WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return Setting(key=row["key"], value=row["value"],
                               updated_at=row["updated_at"])
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения настройки '{key}': {exc}")
            raise

    # ------------------------------------------------------------------
    # Резервное копирование / восстановление
    # ------------------------------------------------------------------
    async def backup_database(self, backup_path: str) -> bool:
        """
        Создание резервной копии файла БД (через SQLite backup API —
        безопасно даже при активных WAL-транзакциях).

        :param backup_path: Путь файла бэкапа.
        """
        conn = self._check_conn()
        try:
            os.makedirs(os.path.dirname(backup_path) or ".", exist_ok=True)
            # Принудительный сброс WAL в основной файл перед копированием
            await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            def _do_backup() -> None:
                src = sqlite3.connect(self.db_path)
                dst = sqlite3.connect(backup_path)
                try:
                    with dst:
                        src.backup(dst)
                finally:
                    src.close()
                    dst.close()

            import asyncio
            await asyncio.get_event_loop().run_in_executor(None, _do_backup)
            logger.info(f"Резервная копия БД создана: {backup_path}")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка создания резервной копии '{backup_path}': {exc}")
            raise

    async def restore_database(self, backup_path: str) -> bool:
        """
        Восстановление БД из резервной копии. Перезапускает соединение.

        :param backup_path: Путь файла бэкапа.
        """
        try:
            if not os.path.exists(backup_path):
                raise FileNotFoundError(f"Файл бэкапа не найден: {backup_path}")

            # Закрываем текущее соединение
            if self.db is not None:
                await self.db.close()
                self.db = None

            # Копируем файл поверх текущего (удаляем WAL/SHM артефакты)
            shutil.copy2(backup_path, self.db_path)
            for suffix in ("-wal", "-shm"):
                artifact = self.db_path + suffix
                if os.path.exists(artifact):
                    os.remove(artifact)

            # Переинициализируем соединение
            await self.initialize()
            logger.info(f"База данных восстановлена из: {backup_path}")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка восстановления БД из '{backup_path}': {exc}")
            # Пытаемся восстановить работоспособное соединение
            try:
                await self.initialize()
            except Exception:  # noqa: BLE001
                pass
            raise
