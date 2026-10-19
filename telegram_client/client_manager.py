# -*- coding: utf-8 -*-
"""
Менеджер Telethon-клиентов приложения Telegram Discovery.

Отвечает за:
- создание подключённых клиентов из сохранённых (зашифрованных) сессий;
- добавление нового аккаунта по схеме: номер телефона -> код -> 2FA;
- шифрование строковой сессии Telethon перед сохранением в БД;
- формирование конфигурации прокси (socks5 / http, включая MTProto)
  через python-socks.

ВАЖНО: все методы асинхронные и должны вызываться внутри asyncio event loop
(в приложении — фоновый loop AsyncBridge), чтобы не блокировать GUI.
"""

import asyncio
import os
import re
from typing import Any, Callable, Coroutine, Optional

from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    AuthKeyDuplicatedError,
    BadRequestError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberAppSignupForbiddenError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    PhoneNotOccupiedError,
    SessionPasswordNeededError,
    YouBlockedUserError,
)
from telethon.network import ConnectionTcpFull
from telethon.sessions import StringSession

# Совместимость с разными версиями Telethon: класс RPC-ошибки транспорта
# может отсутствовать в некоторых сборках telethon.errors — импортируем
# напрямую из lowlevel, а при неудаче обходимся без него.
try:  # Telethon >= 1.24
    from telethon.errors.rpcbaseerrors import RPCError  # noqa: F401
except ImportError:  # pragma: no cover - очень старые версии
    RPCError = None  # type: ignore[assignment]

try:
    from telethon.errors import RPCCallFailedError as _RPCCallFailedError  # type: ignore
except ImportError:
    try:
        from telethon.network.connection.common import RPCCallFailedError as _RPCCallFailedError  # type: ignore
    except ImportError:
        _RPCCallFailedError = None  # type: ignore[assignment]

from database.db import DatabaseManager
from database.models import Account
from utils.encryption import EncryptionManager

# Ключи настроек БД для API-параметров Telegram
KEY_TG_API_ID = "tg_api_id"
KEY_TG_API_HASH = "tg_api_hash"

# Строгое сравнение телефонных номеров (сигнатура Telethon)
_PHONE_RE = re.compile(r"^\+?[0-9]{5,16}$")


class ClientManagerError(Exception):
    """Базовое исключение менеджера клиентов."""


class MissingApiCredentialsError(ClientManagerError):
    """Не заданы api_id / api_hash."""


def format_telethon_error(exc: Exception) -> str:
    """
    Преобразование исключения Telethon в человекочитаемое сообщение (рус.).

    :param exc: Исключение, возникшее при работе с Telegram.
    :return: Текст ошибки для отображения в GUI.
    """
    if isinstance(exc, PhoneCodeInvalidError):
        return "Неверный код подтверждения. Проверьте код из SMS и попробуйте снова."
    if isinstance(exc, PhoneCodeExpiredError):
        return "Срок действия кода истёк. Нажмите «Отправить код» ещё раз."
    if isinstance(exc, PhoneNumberInvalidError):
        return "Неверный формат номера телефона. Используйте международный формат, например +79991234567."
    if isinstance(exc, PhoneNumberBannedError):
        return "Этот номер заблокирован Telegram."
    if isinstance(exc, PhoneNotOccupiedError):
        return (
            "Номер не зарегистрирован в Telegram. Убедитесь, что на этом номере "
            "создан аккаунт Telegram, либо сначала зарегистрируйтесь через официальный клиент."
        )
    if isinstance(exc, PhoneNumberFloodError):
        return (
            "Слишком много запросов кода на этот номер. SMS-доставка временно "
            "заблокирована Telegram — повторите попытку через несколько часов "
            "или с другого IP/прокси."
        )
    if isinstance(exc, PhoneNumberAppSignupForbiddenError):
        return (
            "Telegram запрещает вход по номеру через сторонние приложения. "
            "Попробуйте использовать прокси (лучше MTProto) или другой номер."
        )
    if isinstance(exc, FloodWaitError):
        return f"Слишком много попыток. Повторите через {int(getattr(exc, 'seconds', 0))} сек."
    if isinstance(exc, (ApiIdInvalidError, AuthKeyDuplicatedError)):
        return "Неверные api_id / api_hash или конфликт сессии. Проверьте настройки Telegram API."
    if isinstance(exc, YouBlockedUserError):
        return (
            "Вы заблокировали сервисный аккаунт Telegram (@SpamBot) в другом "
            "приложении — разблокируйте его и повторите."
        )
    # Собираем кортеж типов сетевых/RPC-ошибок с учётом разных версий Telethon
    _net_exc_types: tuple[type, ...] = (ConnectionError, OSError, asyncio.TimeoutError)
    if _RPCCallFailedError is not None:
        _net_exc_types += (_RPCCallFailedError,)
    if RPCError is not None:
        _net_exc_types += (RPCError,)

    if isinstance(exc, _net_exc_types):
        return (
            "Нет соединения с серверами Telegram (ошибка сети).\n\n"
            "Возможные причины:\n"
            "1) Блокировка MTProto-портов (80/443) провайдером или файрволом — "
            "включите прокси (MTProto/socks5) либо VPN;\n"
            "2) Антивирус/файрвол блокирует приложение;\n"
            "3) Неверные api_id/api_hash.\n"
            "Код при этом НЕ отправлялся."
        )
    if isinstance(exc, BadRequestError):
        return f"Telegram отклонил запрос: {exc}"
    return f"Ошибка Telegram: {exc}"


class ClientManager:
    """Менеджер создания клиентов Telethon и управления аккаунтами."""

    def __init__(self, db: DatabaseManager, encryption: EncryptionManager) -> None:
        """
        :param db: Инициализированный менеджер базы данных.
        :param encryption: Менеджер шифрования (мастер-ключ уже загружен).
        """
        self.db: DatabaseManager = db
        self.encryption: EncryptionManager = encryption

    # ------------------------------------------------------------------
    # Параметры API (TG_API_ID / TG_API_HASH)
    # ------------------------------------------------------------------
    async def get_api_credentials(self) -> tuple[int, str]:
        """
        Получение api_id / api_hash: сначала переменные окружения,
        затем глобальные настройки из таблицы settings (tg_api_id/tg_api_hash).

        :return: Кортеж (api_id, api_hash).
        :raises MissingApiCredentialsError: Если параметры нигде не заданы.
        """
        api_id_raw: Optional[str] = os.environ.get("TG_API_ID") or None
        api_hash: Optional[str] = os.environ.get("TG_API_HASH") or None

        if not api_id_raw or not api_hash:
            try:
                db_id = await self.db.get_setting(KEY_TG_API_ID)
                db_hash = await self.db.get_setting(KEY_TG_API_HASH)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка чтения API-настроек из БД: {exc}")
                raise ClientManagerError(
                    "Не удалось прочитать настройки tg_api_id / tg_api_hash из БД"
                ) from exc
            api_id_raw = api_id_raw or (db_id or None)
            api_hash = api_hash or (db_hash or None)

        if not api_id_raw or not api_hash:
            raise MissingApiCredentialsError(
                "Не заданы параметры Telegram API.\n\n"
                "Получите их на https://my.telegram.org/apps и задайте любым способом:\n"
                "1) Переменные окружения TG_API_ID и TG_API_HASH;\n"
                "2) Настройки приложения: ключи tg_api_id и tg_api_hash."
            )

        try:
            api_id = int(str(api_id_raw).strip())
        except ValueError as exc:
            raise MissingApiCredentialsError(
                f"Некорректное значение api_id: «{api_id_raw}». Ожидается целое число."
            ) from exc

        return api_id, str(api_hash).strip()

    # ------------------------------------------------------------------
    # Прокси
    # ------------------------------------------------------------------
    async def build_proxy_config(self, proxy_id: int) -> Optional[dict[str, Any]]:
        """
        Формирование конфига прокси для TelegramClient по записи из таблицы proxies.

        Поддерживаются типы: socks5, socks4, http, mtproxy (MTProto).
        Логин/пароль хранятся в БД в зашифрованном виде и расшифровываются здесь.

        :param proxy_id: Идентификатор прокси в БД.
        :return: Словарь с параметрами proxy/scheme для Telethon либо None.
        """
        try:
            proxy = await self.db.get_proxy(proxy_id)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения прокси id={proxy_id}: {exc}")
            raise ClientManagerError(f"Не удалось получить прокси id={proxy_id}") from exc

        if proxy is None:
            logger.warning(f"Прокси id={proxy_id} не найден в БД")
            return None

        # Расшифровка учётных данных прокси (могут быть пустыми)
        username: Optional[str] = (
            self.encryption.decrypt(proxy.username) if proxy.username else None
        )
        password: Optional[str] = (
            self.encryption.decrypt(proxy.password) if proxy.password else None
        )

        ptype = (proxy.type or "socks5").lower()
        config: dict[str, Any] = {"addr": proxy.host, "port": int(proxy.port)}
        if username:
            config["username"] = username
        if password:
            config["password"] = password

        if ptype in ("mtproxy", "mtproto"):
            # MTProto-прокси: секрет передаётся в поле 'secret' (python-socks)
            config["scheme"] = "mtproxy"
            config["secret"] = username or ""
        elif ptype == "http":
            config["scheme"] = "http"
        elif ptype == "socks4":
            config["scheme"] = "socks4"
        else:  # socks5 / socks — значения по умолчанию для python-socks
            config["scheme"] = "socks5"

        logger.debug(
            f"Сформирован конфиг прокси id={proxy.id} ({proxy.name}): "
            f"{ptype}://{proxy.host}:{proxy.port}"
        )
        return config

    # ------------------------------------------------------------------
    # Создание клиента из сохранённой сессии
    # ------------------------------------------------------------------
    async def create_client(self, account_id: int) -> TelegramClient:
        """
        Создание и подключение Telethon-клиента по аккаунту из БД.

        :param account_id: Идентификатор аккаунта.
        :return: Подключённый TelegramClient.
        :raises ClientManagerError: При отсутствии аккаунта/сессии или ошибке подключения.
        """
        try:
            account = await self.db.get_account(account_id)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка получения аккаунта id={account_id}: {exc}")
            raise ClientManagerError(f"Не удалось получить аккаунт id={account_id}") from exc

        if account is None:
            raise ClientManagerError(f"Аккаунт id={account_id} не найден в базе данных")
        if not account.session_data:
            raise ClientManagerError(
                f"У аккаунта id={account_id} отсутствует сохранённая сессия"
            )

        # Расшифровка строковой сессии
        try:
            session_string = self.encryption.decrypt(account.session_data)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка расшифровки сессии аккаунта id={account_id}: {exc}")
            raise ClientManagerError(
                f"Не удалось расшифровать сессию аккаунта id={account_id}. "
                "Возможно, мастер-ключ был изменён."
            ) from exc

        # ВАЖНО: StringSession НЕ хранит api_id/api_hash — они всегда
        # берутся из настроек/переменных окружения (см. get_api_credentials).
        api_id, api_hash = await self.get_api_credentials()

        proxy_cfg: Optional[dict[str, Any]] = None
        if account.proxy_id is not None:
            proxy_cfg = await self.build_proxy_config(int(account.proxy_id))

        client: Optional[TelegramClient] = None
        try:
            client = TelegramClient(
                StringSession(session_string),
                api_id,
                api_hash,
                proxy=proxy_cfg,
            )
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                raise ClientManagerError(
                    f"Сессия аккаунта id={account_id} не авторизована "
                    "(возможно, аккаунт разлогинен или забанен)"
                )
            logger.info(f"Клиент аккаунта id={account_id} подключён к Telegram")
            return client
        except ClientManagerError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка подключения клиента аккаунта id={account_id}: {exc}")
            if client is not None:
                try:
                    await client.disconnect()
                except Exception as disconnect_exc:  # noqa: BLE001
                    logger.warning(f"Ошибка отключения клиента: {disconnect_exc}")
            raise ClientManagerError(format_telethon_error(exc)) from exc

    # ------------------------------------------------------------------
    # Добавление аккаунта: шаг 1 — отправка кода
    # ------------------------------------------------------------------
    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Нормализация номера телефона: удаляются пробелы, скобки, дефисы.

        :param phone: Введённый пользователем номер.
        :return: Номер в формате +XXXXXXXXXXX (или без +, если его не было).
        """
        cleaned = re.sub(r"[\s\-\(\)]+", "", phone or "")
        return "+" + cleaned.lstrip("+") if cleaned.startswith("+") else cleaned

    @classmethod
    def validate_phone(cls, phone: str) -> bool:
        """Проверка номера на соответствие формату E.164 (упрощённо)."""
        return bool(_PHONE_RE.match(cls.normalize_phone(phone)))

    # ------------------------------------------------------------------
    # Внутренний помощник: временный клиент с таймаутами и статус-колбэками
    # ------------------------------------------------------------------
    async def _run_temp_client(
        self,
        session: "StringSession",
        proxy_cfg: Optional[dict[str, Any]],
        action: Callable[[TelegramClient], Coroutine[Any, Any, Any]],
        on_status: Optional[Callable[[str], None]] = None,
        connect_timeout: float = 25.0,
        request_timeout: float = 35.0,
    ) -> Any:
        """
        Выполнение действия над временным TelegramClient с жёсткими таймаутами.

        Гарантирует:
        - отчёт об этапах через on_status ("connecting"/"connected"/"done"),
          чтобы GUI показывал прогресс и не выглядел «зависшим»;
        - прерывание операции по таймауту (connect / request);
        - гарантированное отключение клиента в finally.

        :param session: Объект StringSession (новый или восстановленный).
        :param proxy_cfg: Конфиг прокси либо None.
        :param action: Асинхронная функция, получающая подключённый клиент.
        :param on_status: Колбэк этапов (вызывается из asyncio-потока).
        :param connect_timeout: Таймаут подключения к серверам Telegram, сек.
        :param request_timeout: Таймаут самого запроса (send_code/sign_in), сек.
        :return: Результат выполнения action.
        """
        api_id, api_hash = await self.get_api_credentials()

        def _status(text: str) -> None:
            """Безопасный вызов колбэка статуса."""
            if on_status is not None:
                try:
                    on_status(text)
                except Exception as cb_exc:  # noqa: BLE001
                    logger.warning(f"Ошибка колбэка статуса: {cb_exc}")

        client: Optional[TelegramClient] = None
        try:
            client = TelegramClient(
                session,
                api_id,
                api_hash,
                proxy=proxy_cfg,
                connection=ConnectionTcpFull,
                request_retries=1,
                connection_retries=1,
                auto_reconnect=False,
            )
            _status("connecting")
            await asyncio.wait_for(client.connect(), timeout=connect_timeout)
            _status("connected")

            result = await asyncio.wait_for(action(client), timeout=request_timeout)
            _status("done")
            return result
        except asyncio.TimeoutError as exc:
            _status("timeout")
            raise ClientManagerError(
                "Превышено время ожидания ответа от серверов Telegram.\n\n"
                "Чаще всего это означает, что соединение заблокировано:\n"
                "1) Попробуйте включить прокси (MTProto/socks5) в этом диалоге;\n"
                "2) Или используйте VPN;\n"
                "3) Проверьте антивирус/файрвол — он может блокировать приложение.\n"
                "Код НЕ был отправлен."
            ) from exc
        finally:
            if client is not None:
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=10.0)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Ошибка отключения временного клиента: {exc}")

    async def add_account_by_phone(
        self,
        phone: str,
        proxy_id: Optional[int] = None,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> dict[str, str]:
        """
        Шаг 1 добавления аккаунта: отправка кода подтверждения на номер.

        Создаёт временного клиента с новой строковой сессией, подключается
        к Telegram и вызывает send_code_request. Клиент после отправки
        отключается; сессия возвращается в вызывающий код (диалог) для шага 2.

        ВАЖНО про SMS: Telegram сначала пытается доставить код в другие
        устройства (официальные приложения), затем звонком или SMS. Если у
        аккаунта включена 2FA, SMS может не приходить вовсе — в этом случае
        на шаге 2 достаточно оставить поле кода пустым и ввести облачный
        пароль (см. complete_login).

        :param phone: Номер телефона в международном формате (+7...).
        :param proxy_id: Идентификатор прокси (опционально).
        :param on_status: Колбэк этапов ("connecting"/"connected"/...) для GUI.
        :return: Словарь {"status": "code_sent", "phone_code_hash": hash,
                 "session_string": строковая сессия}.
        :raises ClientManagerError: При ошибках валидации/сети/FloodWait.
        """
        normalized = self.normalize_phone(phone)
        if not _PHONE_RE.match(normalized):
            raise ClientManagerError(
                "Неверный формат номера телефона. Ожидается международный "
                "формат, например +79991234567."
            )

        proxy_cfg = (
            await self.build_proxy_config(int(proxy_id))
            if proxy_id is not None
            else None
        )

        session = StringSession()

        async def _send_code(client: TelegramClient) -> dict[str, str]:
            """Отправка кода и извлечение сессии ДО disconnect()."""
            sent = await client.send_code_request(normalized)
            # ВАЖНО: извлекаем строковую сессию ДО отключения клиента.
            # При disconnect() Telethon очищает серверный nonce авторизации,
            # и последующий sign_in на шаге 2 завершался бы ошибкой.
            result_session = StringSession.save(client.session)
            logger.info(
                f"Код подтверждения запрошен для номера "
                f"{normalized[:3]}***{normalized[-2:]}"
            )
            return {
                "status": "code_sent",
                "phone_code_hash": sent.phone_code_hash or "",
                "session_string": result_session,
            }

        try:
            return await self._run_temp_client(
                session, proxy_cfg, _send_code, on_status=on_status
            )
        except ClientManagerError:
            raise
        except FloodWaitError as exc:
            logger.warning(f"FloodWait при отправке кода: {exc.seconds} сек.")
            raise ClientManagerError(format_telethon_error(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка отправки кода на {normalized[:3]}***: {exc}")
            raise ClientManagerError(format_telethon_error(exc)) from exc

    # ------------------------------------------------------------------
    # Добавление аккаунта: шаг 2 — завершение входа
    # ------------------------------------------------------------------
    async def complete_login(
        self,
        phone: str,
        code: str,
        session_string: str,
        phone_code_hash: str,
        password_2fa: Optional[str] = None,
        proxy_id: Optional[int] = None,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> Account:
        """
        Шаг 2 добавления аккаунта: ввод кода (и 2FA при необходимости),
        получение строковой сессии, её шифрование и сохранение аккаунта в БД.

        Поддерживается вход ТОЛЬКО по облачному паролю (2FA): если поле
        ``code`` пустое, выполняется немедленный вызов sign_in(password=...)
        без SMS-кода — Telegram это разрешает для аккаунтов с включённой
        двухфакторной аутентификацией (SMS при этом не приходит намеренно).

        :param phone: Номер телефона (тот же, что использовался для отправки кода).
        :param code: Код подтверждения из SMS/Telegram; пусто = вход по 2FA-паролю.
        :param session_string: Строковая сессия, полученная на шаге 1.
        :param phone_code_hash: Хэш отправленного кода.
        :param password_2fa: Пароль двухфакторной аутентификации (облачный пароль).
        :param proxy_id: Идентификатор прокси (опционально).
        :param on_status: Колбэк этапов ("connecting"/"connected"/...) для GUI.
        :return: Созданная запись Account (id заполнен).
        :raises ClientManagerError: При неверном коде, истёкшем коде, ошибке 2FA и т.п.
        """
        normalized = self.normalize_phone(phone)
        clean_code = (code or "").strip()
        clean_password = (password_2fa or "").strip() or None

        if not clean_code and not clean_password:
            raise ClientManagerError(
                "Введите код подтверждения либо облачный пароль (2FA)."
            )

        proxy_cfg = (
            await self.build_proxy_config(int(proxy_id))
            if proxy_id is not None
            else None
        )

        async def _sign_in_and_save(client: TelegramClient) -> Account:
            """Выполнение sign_in и сохранение аккаунта в БД."""
            try:
                if clean_code:
                    # Обычный вход: код (+ 2FA, если потребуется)
                    try:
                        await client.sign_in(
                            phone=normalized,
                            code=clean_code,
                            phone_code_hash=phone_code_hash,
                        )
                    except SessionPasswordNeededError:
                        # Требуется облачный пароль (2FA)
                        if not clean_password:
                            raise ClientManagerError(
                                "Для этого аккаунта включена двухфакторная "
                                "аутентификация. Введите облачный пароль (2FA) "
                                "и повторите."
                            )
                        await client.sign_in(password=clean_password)
                else:
                    # Вход СРАЗУ по облачному паролю, без SMS-кода.
                    # Работает для аккаунтов с включённой 2FA — именно этот
                    # способ нужно использовать, когда SMS не приходят.
                    await client.sign_in(password=clean_password)
            except ClientManagerError:
                raise

            # Проверка авторизации
            me = await client.get_me()
            if me is None:
                raise ClientManagerError("Telegram не вернул данные пользователя.")

            # Сохранение строковой сессии (StringSession.save возвращает строку)
            saved_session_string = StringSession.save(client.session)
            if not saved_session_string:
                raise ClientManagerError("Не удалось сохранить строковую сессию.")

            # Шифрование сессии перед записью в БД
            encrypted_session = self.encryption.encrypt(saved_session_string)

            username = getattr(me, "username", None)
            account = Account(
                phone=normalized,
                username=username,
                session_data=encrypted_session,
                proxy_id=proxy_id,
                status="active",
                last_used=None,
            )
            account_id = await self.db.create_account(account)
            account.id = account_id

            logger.info(
                f"Аккаунт создан: id={account_id}, username=@{username}, "
                f"телефон {normalized[:3]}***{normalized[-2:]}"
            )
            return account

        try:
            return await self._run_temp_client(
                StringSession(session_string),
                proxy_cfg,
                _sign_in_and_save,
                on_status=on_status,
            )
        except ClientManagerError:
            raise
        except FloodWaitError as exc:
            logger.warning(f"FloodWait при входе: {exc.seconds} сек.")
            raise ClientManagerError(format_telethon_error(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка завершения входа для {normalized[:3]}***: {exc}")
            raise ClientManagerError(format_telethon_error(exc)) from exc

    # ------------------------------------------------------------------
    # Добавление аккаунта: импорт готовой строковой сессии (StringSession)
    # ------------------------------------------------------------------
    async def add_account_by_session(
        self,
        session_string: str,
        proxy_id: Optional[int] = None,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> Account:
        """
        Добавление аккаунта импортом готовой строковой сессии Telethon.

        Способ не требует SMS и api_id/api_hash: строковая сессия Telethon
        уже содержит все параметры авторизации вместе с api_id/api_hash,
        использованными при её создании. Достаточно проверить её валидность
        подключением к Telegram и сохранить (в зашифрованном виде) в БД.

        :param session_string: Строка вида «1A2B...» из StringSession.
        :param proxy_id: Идентификатор прокси (опционально).
        :param on_status: Колбэк этапов для GUI.
        :return: Созданная запись Account (id заполнен).
        :raises ClientManagerError: При пустой/битой сессии или ошибке сети.
        """
        clean_session = (session_string or "").strip().strip('"').strip("'")
        if len(clean_session) < 50:
            raise ClientManagerError(
                "Похоже, строковая сессия обрезана или пуста. Скопируйте её "
                "целиком (обычно 300+ символов, начинается с цифры)."
            )

        proxy_cfg = (
            await self.build_proxy_config(int(proxy_id))
            if proxy_id is not None
            else None
        )

        async def _verify_and_save(client: TelegramClient) -> Account:
            """Проверка авторизации и сохранение сессии в БД."""
            me = await client.get_me()
            if me is None:
                raise ClientManagerError(
                    "Сессия не авторизована: Telegram не вернул данные "
                    "пользователя. Сгенерируйте новую StringSession."
                )

            # Переупаковываем сессию (гарантирует валидную строку Telethon)
            saved_session_string = StringSession.save(client.session)
            encrypted_session = self.encryption.encrypt(saved_session_string)

            username = getattr(me, "username", None)
            phone = getattr(me, "phone", None)

            account = Account(
                phone=phone,
                username=username,
                session_data=encrypted_session,
                proxy_id=proxy_id,
                status="active",
                last_used=None,
            )
            account_id = await self.db.create_account(account)
            account.id = account_id

            logger.info(
                f"Аккаунт добавлен импортом сессии: id={account_id}, "
                f"username=@{username or '—'}"
            )
            return account

        try:
            return await self._run_temp_client(
                StringSession(clean_session),
                proxy_cfg,
                _verify_and_save,
                on_status=on_status,
            )
        except ClientManagerError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка импорта строковой сессии: {exc}")
            raise ClientManagerError(format_telethon_error(exc)) from exc

    # ------------------------------------------------------------------
    # Добавление аккаунта: импорт TData (Unigram/AyuGram/Telegram Desktop)
    # ------------------------------------------------------------------
    @staticmethod
    def find_tdata_accounts(base_path: str) -> list[tuple[str, str]]:
        """
        Поиск папок tdata с аккаунтами внутри указанного пути.

        Структура профиля Unigram-клиентов (Telegram Desktop, AyuGram, iGram):
        <профиль>/tdata/<имя_аккаунта>/{key_datas, map, ...}

        :param base_path: Путь к папке с tdata ЛИБО к AppData-папке клиента
                          (например C:\\Users\\You\\AppData\\Roaming\\AyuGramDesktop).
        :return: Список пар (имя аккаунта, путь к папке tdata аккаунта).
        """
        base = os.path.abspath((base_path or "").strip().strip('"'))
        found: list[tuple[str, str]] = []

        def _is_tdata_dir(path: str) -> bool:
            """Папка tdata аккаунта — содержит файл key_datas."""
            return os.path.isdir(path) and os.path.isfile(
                os.path.join(path, "key_datas")
            )

        if not os.path.isdir(base):
            return found

        # Вариант 1: сам указанный путь — папка tdata аккаунта
        if _is_tdata_dir(base) or (
            os.path.basename(base).lower() == "tdata" and _is_tdata_dir(os.path.dirname(base))
        ):
            target = base if _is_tdata_dir(base) else os.path.dirname(base)
            found.append((os.path.basename(target), target))
            return found

        # Вариант 2: base — AppData-папка клиента: ищем <base>/tdata/*
        tdata_root = os.path.join(base, "tdata")
        if os.path.isdir(tdata_root):
            for name in sorted(os.listdir(tdata_root)):
                acc_dir = os.path.join(tdata_root, name)
                if _is_tdata_dir(acc_dir):
                    found.append((name, acc_dir))
            if found:
                return found

        # Вариант 2b: base — сама корневая папка tdata: <base>/<аккаунт>/key_datas
        if os.path.basename(base).lower() == "tdata":
            for name in sorted(os.listdir(base)):
                acc_dir = os.path.join(base, name)
                if _is_tdata_dir(acc_dir):
                    found.append((name, acc_dir))
            if found:
                return found

        # Вариант 3: рекурсивный поиск папок tdata (ограниченная глубина)
        try:
            for root, dirs, _files in os.walk(base):
                # Не спускаемся в глубокие системные каталоги
                depth = os.path.relpath(root, base).count(os.sep)
                if depth > 4:
                    dirs.clear()
                    continue
                for d in list(dirs):
                    if d.lower() == "tdata":
                        td_path = os.path.join(root, d)
                        for name in sorted(os.listdir(td_path)):
                            acc_dir = os.path.join(td_path, name)
                            if _is_tdata_dir(acc_dir):
                                found.append((name, acc_dir))
                        dirs.remove(d)
                if found:
                    break
        except OSError as exc:
            logger.warning(f"Ошибка сканирования {base}: {exc}")

        return found

    async def add_account_by_tdata(
        self,
        tdata_path: str,
        password: Optional[str] = None,
        proxy_id: Optional[int] = None,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> Account:
        """
        Добавление аккаунта конвертацией TData-сессии десктопного клиента.

        Поддерживаются AyuGram / iGram / официальный Telegram Desktop
        (все на движке Unigram). Конвертация выполняет пакет opentele;
        полученная строковая сессия Telethon шифруется и сохраняется в БД.

        ВАЖНО: на время конвертации рекомендуется полностью закрыть
        десктопный клиент (иначе файлы tdata могут быть заняты).

        :param tdata_path: Путь к папке tdata конкретного аккаунта
                           (содержит файлы key_datas, map).
        :param password: Пароль локального шифрования tdata (если задан
                         в настройках клиента), иначе None.
        :param proxy_id: Идентификатор прокси для аккаунта (опционально).
        :param on_status: Колбэк этапов для GUI.
        :return: Созданная запись Account (id заполнен).
        :raises ClientManagerError: Если opentele не установлен, путь неверен
                                    или конвертация/подключение не удалось.
        """
        # 1. Зависимость opentele (тяжёлый импорт — только по требованию)
        try:
            from opentele.tdata import TDataSession  # type: ignore
        except ImportError as exc:
            raise ClientManagerError(
                "Для импорта TData необходим пакет opentele.\n\n"
                "Установите его командой:\n"
                "    pip install opentele\n\n"
                "или через меню приложения, если установка доступна."
            ) from exc

        # 2. Валидация пути
        abs_path = os.path.abspath((tdata_path or "").strip().strip('"'))
        if not os.path.isdir(abs_path):
            raise ClientManagerError(
                f"Папка не найдена: {abs_path}\n\n"
                "Нужно указать путь к папке tdata КОНКРЕТНОГО аккаунта, "
                "например:\n"
                "C:\\Users\\ВашНик\\AppData\\Roaming\\AyuGramDesktop\\tdata\\Work"
            )
        if not any(
            os.path.exists(os.path.join(abs_path, f))
            for f in ("key_datas", "map", "settings")
        ):
            raise ClientManagerError(
                f"В папке {abs_path} нет файлов сессии Telegram "
                "(key_datas / map).\n\nУкажите путь к подпапке аккаунта "
                "внутри tdata (например ...\\tdata\\Work)."
            )

        def _status(text: str) -> None:
            """Безопасный вызов колбэка статуса."""
            if on_status is not None:
                try:
                    on_status(text)
                except Exception as cb_exc:  # noqa: BLE001
                    logger.warning(f"Ошибка колбэка статуса TData: {cb_exc}")

        # 3. Конвертация TData -> StringSession (CPU-задача, в отдельном потоке)
        _status("tdata_convert")
        try:
            td_kwargs: dict[str, Any] = {"folder": abs_path}
            if password:
                td_kwargs["password"] = password
            tdata_session = await asyncio.to_thread(TDataSession, **td_kwargs)
            session_string: str = await asyncio.to_thread(
                lambda: str(tdata_session.migrate_to_string())
            )
        except ValueError as exc:
            # opentele бросает ValueError при неверном пароле tdata
            if "password" in str(exc).lower():
                raise ClientManagerError(
                    "Неверный пароль локального шифрования tdata. "
                    "Задайте его в поле «Пароль tdata» или оставьте пустым, "
                    "если пароль не устанавливался."
                ) from exc
            logger.error(f"Ошибка конвертации TData: {exc}")
            raise ClientManagerError(f"Не удалось прочитать TData: {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка конвертации TData из {abs_path}: {exc}")
            raise ClientManagerError(
                f"Ошибка конвертации TData: {exc}\n\n"
                "Совет: полностью закройте десктопный Telegram/AyuGram и "
                "повторите попытку (файлы tdata могут быть заняты)."
            ) from exc

        if not session_string:
            raise ClientManagerError("Конвертация не вернула строковую сессию.")

        _status("tdata_converted")

        # 4. Проверка сессии и сохранение аккаунта в БД
        account = await self.add_account_by_session(
            session_string, proxy_id=proxy_id, on_status=on_status
        )
        return account

    # ------------------------------------------------------------------
    # Проверка работоспособности аккаунта (используется вкладкой Аккаунты)
    # ------------------------------------------------------------------
    async def check_account(self, account_id: int) -> str:
        """
        Подключение к Telegram по аккаунту для проверки валидности сессии.

        :param account_id: Идентификатор аккаунта.
        :return: Сообщение о результате (для панели логов).
        """
        client: Optional[TelegramClient] = None
        try:
            client = await self.create_client(account_id)
            me = await client.get_me()
            username = getattr(me, "username", None) or "без username"
            return f"Аккаунт id={account_id} (@{username}) — сессия валидна"
        except ClientManagerError as exc:
            # Помечаем аккаунт как проблемный
            try:
                account = await self.db.get_account(account_id)
                if account is not None:
                    account.status = "invalid"
                    await self.db.update_account(account)
            except Exception as upd_exc:  # noqa: BLE001
                logger.error(f"Не удалось обновить статус аккаунта id={account_id}: {upd_exc}")
            return f"Аккаунт id={account_id} — ошибка: {exc}"
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Ошибка отключения клиента: {exc}")
