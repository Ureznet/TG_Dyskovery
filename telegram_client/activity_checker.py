# -*- coding: utf-8 -*-
"""
Модуль проверки активности Telegram-каналов через MTProto (Telethon).

Класс ActivityChecker выполняет для каждого канала:
- получение сущности (id, название, количество подписчиков, тип доступа);
- определение даты последнего поста (get_messages limit=1);
- определение даты последнего комментария (в discussion-группе канала);
- расчёт признака активности по порогу из глобальных настроек;
- сохранение результата в таблицу checker_results (текст сообщений НЕ сохраняется).

Все методы асинхронные; вызывающий код (GUI) оборачивает их в фоновый
asyncio-поток, чтобы не блокировать интерфейс.
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from loguru import logger
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Channel as TLChannel

from database.db import DatabaseManager
from database.models import CheckerResult
from database.settings import SettingsManager
from telegram_client.client_manager import ClientManager


def normalize_username(raw: str) -> str:
    """
    Нормализация строки канала к виду '@username'.

    Поддерживаются форматы ввода: '@name', 't.me/name', 'https://t.me/name',
    'www.t.me/name', а также ссылки на публикации 't.me/name/123'
    (ссылки разрешаются Telethon автоматически).

    :param raw: Исходная строка из списка пользователя.
    :return: Нормализованный username либо пустая строка, если распознать не удалось.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    # Отрезаем протокол и домен
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    if s.startswith("www."):
        s = s[len("www."):]
    if s.startswith("t.me/"):
        s = s[len("t.me/"):]
    # Убираем завершающий слэш
    s = s.rstrip("/")
    if not s:
        return ""
    if not s.startswith("@"):
        s = "@" + s
    return s


class ActivityChecker:
    """Чекер активности каналов через MTProto (Telethon)."""

    def __init__(
        self,
        client_manager: ClientManager,
        db: DatabaseManager,
        settings: SettingsManager,
    ) -> None:
        """
        :param client_manager: Менеджер Telethon-клиентов (создание клиентов).
        :param db: Менеджер базы данных (сохранение результатов).
        :param settings: Менеджер настроек (порог активности, задержки).
        """
        self.client_manager: ClientManager = client_manager
        self.db: DatabaseManager = db
        self.settings: SettingsManager = settings

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------
    @staticmethod
    def _to_iso(dt: Optional[datetime]) -> Optional[str]:
        """Преобразование datetime в ISO-строку (UTC)."""
        if dt is None:
            return None
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Ошибка преобразования даты {dt} в ISO: {exc}")
            return None

    @staticmethod
    def _is_active_by_threshold(
        last_post_iso: Optional[str],
        last_comment_iso: Optional[str],
        threshold_days: int,
    ) -> bool:
        """
        Расчёт признака активности: последний пост ИЛИ последний комментарий
        моложе порога (threshold_days) — канал считается активным.

        :param last_post_iso: ISO-дата последнего поста (или None).
        :param last_comment_iso: ISO-дата последнего комментария (или None).
        :param threshold_days: Порог активности в днях.
        :return: True, если канал активен.
        """
        now = datetime.now(timezone.utc)
        for iso in (last_post_iso, last_comment_iso):
            if not iso:
                continue
            try:
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_days = (now - dt).total_seconds() / 86400.0
                if age_days <= float(threshold_days):
                    return True
            except ValueError as exc:
                logger.warning(f"Некорректный формат даты «{iso}»: {exc}")
        return False

    @staticmethod
    def determine_access_type(entity: Any) -> str:
        """
        Определение типа доступа к каналу по его атрибутам.

        - публичный канал (имеет username) → 'public';
        - приватный, но доступный нашему аккаунту → 'private_has_access'.

        :param entity: Объект сущности Telethon (Channel/Chat/User).
        :return: Строка типа доступа.
        """
        username = getattr(entity, "username", None)
        if username:
            return "public"
        return "private_has_access"

    async def _get_last_comment_date(self, client: Any, entity: Any) -> Optional[datetime]:
        """
        Получение даты последнего комментария из discussion-группы канала.

        :param client: Подключённый Telethon-клиент.
        :param entity: Сущность канала.
        :return: datetime последнего сообщения в группе обсуждения или None.
        """
        discuss_id = getattr(entity, "discussion_channel_id", None)
        megagroup_id = getattr(entity, "megagroup_id", None)
        target_id: Optional[int] = discuss_id or (
            -100_000_000_000 - megagroup_id if megagroup_id else None
        )
        if not target_id:
            # Группа обсуждения не подключена к каналу
            return None
        try:
            msg = await client.get_messages(target_id, limit=1)
            if isinstance(msg, list):
                msg = msg[0] if msg else None
            if msg is not None and getattr(msg, "date", None) is not None:
                return msg.date
        except ChannelPrivateError:
            # Нет доступа к группе обсуждения — комментариев просто нет
            logger.debug(f"Нет доступа к обсуждению канала id={getattr(entity, 'id', '?')}")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"Ошибка получения обсуждения канала id={getattr(entity, 'id', '?')}: {exc}"
            )
        return None

    # ------------------------------------------------------------------
    # Проверка одного канала
    # ------------------------------------------------------------------
    async def check_channel(self, username: str, account_id: int) -> dict[str, Any]:
        """
        Проверка активности одного канала через MTProto.

        ВАЖНО: клиент создаётся и ОТКЛЮЧАЕТСЯ внутри метода — вызывающий код
        (пакетная проверка) управляет переиспользованием соединений самостоятельно.

        :param username: Канал в любом удобном формате (@name / t.me/name).
        :param account_id: Идентификатор аккаунта Telegram, выполняющего запрос.
        :return: Словарь результата:
                 {username, tg_id, title, access_type, participants_count,
                  last_post_date, last_comment_date, is_active, error_message}.
                 При FloodWait дополнительно: {"error": "FloodWait Ns", "retry_after": N}.
        """
        normalized = normalize_username(username)
        result: dict[str, Any] = {
            "username": normalized or (username or "").strip(),
            "tg_id": None,
            "title": None,
            "access_type": None,
            "participants_count": None,
            "last_post_date": None,
            "last_comment_date": None,
            "is_active": False,
            "error_message": None,
        }
        if not normalized:
            result["error_message"] = "Пустой или некорректный username канала"
            return result

        threshold_days: int = await self.settings.get_activity_threshold_days()

        client = None
        try:
            client = await self.client_manager.create_client(account_id)

            # --- Получение сущности канала ---
            try:
                entity = await client.get_entity(normalized)
            except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as exc:
                # Приватный/недоступный канал: фиксируем тип доступа без дат
                result["access_type"] = "private_no_access"
                result["error_message"] = (
                    f"Канал приватен или недоступен: {exc}"
                    if isinstance(exc, ChannelPrivateError)
                    else f"Канал не найден: {normalized}"
                )
                logger.info(
                    f"{normalized}: нет доступа ({type(exc).__name__})"
                )
                return result

            # --- Основные метаданные канала ---
            result["tg_id"] = getattr(entity, "id", None)
            result["title"] = getattr(entity, "title", None) or \
                getattr(entity, "username", None) or normalized.lstrip("@")
            result["participants_count"] = int(getattr(entity, "participants_count", 0) or 0)
            result["access_type"] = self.determine_access_type(entity)

            # --- Дата последнего поста (только для каналов/супергрупп) ---
            if isinstance(entity, TLChannel) and not getattr(entity, "megagroup", False):
                try:
                    # get_messages принимает и объект сущности, и id
                    messages = await client.get_messages(entity.id, limit=1)
                    last_msg = messages[0] if isinstance(messages, list) and messages else messages
                    if last_msg is not None and getattr(last_msg, "date", None) is not None:
                        result["last_post_date"] = self._to_iso(last_msg.date)
                except ChannelPrivateError:
                    result["access_type"] = "private_no_access"
                    return result
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"{normalized}: ошибка получения последнего поста: {exc}")

                # --- Дата последнего комментария (discussion) ---
                try:
                    comment_dt = await self._get_last_comment_date(client, entity)
                    if comment_dt is not None:
                        result["last_comment_date"] = self._to_iso(comment_dt)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"{normalized}: ошибка получения обсуждения: {exc}")

            # --- Расчёт активности по порогу ---
            result["is_active"] = self._is_active_by_threshold(
                result["last_post_date"],
                result["last_comment_date"],
                threshold_days,
            )

            logger.info(
                f"{normalized}: доступ={result['access_type']}, "
                f"подписчиков={result['participants_count']}, "
                f"активен={result['is_active']}"
            )
            return result

        except FloodWaitError as exc:
            seconds = int(getattr(exc, "seconds", 0) or 0)
            logger.warning(f"FloodWait при проверке {normalized}: {seconds} сек.")
            result["error"] = f"FloodWait {seconds}s"
            result["retry_after"] = seconds
            result["error_message"] = f"FloodWait {seconds}s"
            return result
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка проверки канала {normalized}: {exc}")
            result["error_message"] = str(exc)
            return result
        finally:
            # Клиент, созданный внутри метода, всегда отключается
            if client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Ошибка отключения клиента: {exc}")

    # ------------------------------------------------------------------
    # Сохранение результата в БД
    # ------------------------------------------------------------------
    async def save_result(self, res: dict[str, Any]) -> None:
        """
        Сохранение результата проверки в таблицу checker_results.

        Ошибки сохранения логируются, но не прерывают пакетную проверку.

        :param res: Словарь-результат метода check_channel.
        """
        try:
            record = CheckerResult(
                username=res.get("username"),
                tg_id=res.get("tg_id"),
                title=res.get("title"),
                access_type=res.get("access_type"),
                participants_count=int(res.get("participants_count") or 0),
                last_post_date=res.get("last_post_date"),
                last_comment_date=res.get("last_comment_date"),
                is_active=1 if res.get("is_active") else 0,
                checked_at=datetime.now(timezone.utc).isoformat(),
            )
            await self.db.create_checker_result(record)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Не удалось сохранить результат чекера: {exc}")

    # ------------------------------------------------------------------
    # Пакетная проверка
    # ------------------------------------------------------------------
    async def check_batch(
        self,
        usernames: list[str],
        account_ids: list[int],
        on_result: Optional[Callable[[dict[str, Any]], Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Пакетная проверка списка каналов.

        Особенности:
        - каналы распределяются между аккаунтами round-robin;
        - соединения аккаунтов переиспользуются (low-level проверка без
          пересоздания клиента на каждый канал);
        - между запросами применяется случайная задержка
          random.uniform(delay_min, delay_max) из настроек;
        - ошибка на одном канале не прерывает проверку остальных;
        - при FloodWait аккаунт временно исключается из ротации на
          (retry_after + 5) секунд.

        :param usernames: Список каналов (@username / t.me/username).
        :param account_ids: Список идентификаторов аккаунтов для проверки.
        :param on_result: Асинхронный колбэк, вызываемый после каждого
                          результата (для потокового обновления GUI/БД).
        :return: Список словарей-результатов (по одному на канал).
        """
        if not usernames:
            return []
        if not account_ids:
            raise ValueError("Не выбран ни один аккаунт для проверки")

        delay_min: float = await self.settings.get_default_delay_min()
        delay_max: float = await self.settings.get_default_delay_max()

        results: list[dict[str, Any]] = []

        # Пул клиентов: acc_id -> TelegramClient | None (None = отключён/забанен)
        pool: dict[int, Any] = {}
        # Время, до которого аккаунт «спит» из-за FloodWait
        cooldown_until: dict[int, float] = {}

        async def get_client(acc_id: int) -> Optional[Any]:
            """Ленивое создание/переиспользование клиента аккаунта."""
            if acc_id in pool and pool[acc_id] is not None:
                return pool[acc_id]
            try:
                client = await self.client_manager.create_client(acc_id)
                pool[acc_id] = client
                return client
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Аккаунт id={acc_id} недоступен: {exc}")
                pool[acc_id] = None  # больше не пробуем
                return None

        threshold_days: int = await self.settings.get_activity_threshold_days()

        try:
            for index, raw_name in enumerate(usernames):
                normalized = normalize_username(raw_name)
                res: dict[str, Any] = {
                    "username": normalized or (raw_name or "").strip(),
                    "tg_id": None,
                    "title": None,
                    "access_type": None,
                    "participants_count": None,
                    "last_post_date": None,
                    "last_comment_date": None,
                    "is_active": False,
                    "error_message": None,
                }

                # Round-robin выбор аккаунта
                start_idx = index % len(account_ids)
                chosen_id: Optional[int] = None
                for offset in range(len(account_ids)):
                    candidate = account_ids[(start_idx + offset) % len(account_ids)]
                    # Пропускаем аккаунты на кулдауне FloodWait
                    if cooldown_until.get(candidate, 0.0) > asyncio.get_running_loop().time():
                        continue
                    chosen_id = candidate
                    break

                if chosen_id is None:
                    # Все аккаунты на паузе — ждём до ближайшего выхода из кулдауна
                    wake_at = min(cooldown_until.values(), default=0.0)
                    wait_s = max(1.0, wake_at - asyncio.get_running_loop().time())
                    logger.warning(
                        f"Все аккаунты в FloodWait, ожидание {wait_s:.0f} сек."
                    )
                    await asyncio.sleep(wait_s)
                    chosen_id = account_ids[start_idx]

                client = await get_client(chosen_id)
                if client is None:
                    res["error_message"] = "Нет доступных аккаунтов для проверки"
                    results.append(res)
                    if on_result is not None:
                        await on_result(res)
                    continue

                # --- Проверка канала выбранным клиентом ---
                try:
                    if not normalized:
                        raise ValueError("Пустой или некорректный username канала")

                    try:
                        entity = await client.get_entity(normalized)
                    except (
                        ChannelPrivateError,
                        UsernameNotOccupiedError,
                        UsernameInvalidError,
                    ) as exc:
                        res["access_type"] = "private_no_access"
                        res["error_message"] = (
                            f"Канал приватен или недоступен: {exc}"
                            if isinstance(exc, ChannelPrivateError)
                            else f"Канал не найден: {normalized}"
                        )
                        raise _SkipChannel(res) from exc

                    res["tg_id"] = getattr(entity, "id", None)
                    res["title"] = getattr(entity, "title", None) or \
                        getattr(entity, "username", None) or normalized.lstrip("@")
                    res["participants_count"] = int(
                        getattr(entity, "participants_count", 0) or 0
                    )
                    res["access_type"] = self.determine_access_type(entity)

                    if isinstance(entity, TLChannel) and not getattr(
                        entity, "megagroup", False
                    ):
                        try:
                            # get_messages принимает и объект сущности, и id
                            messages = await client.get_messages(entity.id, limit=1)
                            last_msg = (
                                messages[0]
                                if isinstance(messages, list) and messages
                                else messages
                            )
                            if last_msg is not None and getattr(last_msg, "date", None):
                                res["last_post_date"] = self._to_iso(last_msg.date)
                        except ChannelPrivateError as exc:
                            res["access_type"] = "private_no_access"
                            res["error_message"] = f"Канал приватен: {exc}"
                            raise _SkipChannel(res) from exc

                        comment_dt = await self._get_last_comment_date(client, entity)
                        if comment_dt is not None:
                            res["last_comment_date"] = self._to_iso(comment_dt)

                    res["is_active"] = self._is_active_by_threshold(
                        res["last_post_date"], res["last_comment_date"], threshold_days
                    )
                    logger.info(
                        f"[{index + 1}/{len(usernames)}] {res['username']}: "
                        f"активен={res['is_active']} (аккаунт id={chosen_id})"
                    )
                except _SkipChannel:
                    pass  # результат уже заполнен в res
                except FloodWaitError as exc:
                    seconds = int(getattr(exc, "seconds", 0) or 0)
                    logger.warning(
                        f"FloodWait на аккаунте id={chosen_id}: {seconds} сек."
                    )
                    res["error"] = f"FloodWait {seconds}s"
                    res["retry_after"] = seconds
                    res["error_message"] = f"FloodWait {seconds}s"
                    # Аккаунт уходит на кулдаун
                    cooldown_until[chosen_id] = (
                        asyncio.get_running_loop().time() + seconds + 5.0
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"Ошибка проверки {res['username']}: {exc}")
                    res["error_message"] = str(exc)

                results.append(res)

                # Потоковое сохранение и уведомление GUI о каждом результате
                if on_result is not None:
                    try:
                        await on_result(res)
                    except Exception as cb_exc:  # noqa: BLE001
                        logger.error(f"Ошибка колбэка результата: {cb_exc}")

                # Случайная задержка между запросами (кроме последнего канала)
                if index < len(usernames) - 1:
                    pause = random.uniform(delay_min, delay_max)
                    await asyncio.sleep(pause)
        finally:
            # Отключение всех клиентов пула
            for acc_id, client in pool.items():
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            f"Ошибка отключения клиента аккаунта id={acc_id}: {exc}"
                        )

        return results


class _SkipChannel(Exception):
    """Внутренний сигнал: проверка канала завершена досрочно (результат в self.res)."""

    def __init__(self, res: dict[str, Any]) -> None:
        super().__init__(res.get("error_message") or "skip")
        self.res = res
