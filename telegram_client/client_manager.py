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

    async def _get_account_credentials(self, account_id: int) -> tuple[int, str]:
        """
        Получение api_id/api_hash для конкретного аккаунта.

        Приоритет: служебные настройки tg_api_id_acc_<id>/tg_api_hash_acc_<id>,
        сохранённые при импорте TData (auth-ключ сессии жёстко привязан к
        api_id того клиента, откуда импорт). Если их нет — глобальные
        настройки/переменные окружения (get_api_credentials).

        :param account_id: Идентификатор аккаунта в БД.
        :return: Кортеж (api_id, api_hash).
        """
        try:
            acc_id_raw = await self.db.get_setting(f"tg_api_id_acc_{account_id}")
            acc_hash = await self.db.get_setting(f"tg_api_hash_acc_{account_id}")
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"Ошибка чтения служебных API-настроек аккаунта "
                f"id={account_id}: {exc}"
            )
            acc_id_raw, acc_hash = None, None

        if acc_id_raw and acc_hash:
            try:
                return int(str(acc_id_raw).strip()), str(acc_hash).strip()
            except ValueError:
                logger.warning(
                    f"Некорректные служебные API-настройки аккаунта id={account_id} — "
                    "использую глобальные"
                )
        return await self.get_api_credentials()

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
        # Исключение: аккаунты, импортированные из TData — для них при
        # импорте сохраняются служебные настройки tg_api_id_acc_<id>,
        # соответствующие api-парам того самого tdata (auth-ключ сессии
        # жёстко привязан к api_id, чужие ключи не подойдут).
        api_id, api_hash = await self._get_account_credentials(account_id)

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
    def _read_tdata_accounts_count(
        base_path: str, password: Optional[str]
    ) -> int:
        """
        Подсчёт реально загружаемых аккаунтов в контейнере tdata.

        Читает заголовок key_data/key_datas БЕЗ полной расшифровки:
        localKey извлекается из key*-файла (при наличии passcode — через
        PBKDF2), затем расшифровывается список индексов аккаунтов.

        :param base_path: Корневая папка tdata.
        :param password: Локальный пароль (passcode), если задан.
        :return: Число аккаунтов в контейнере; 0 — если прочитать не удалось.
        """
        try:
            from PySide6.QtCore import QByteArray  # type: ignore
            from opentele import td  # type: ignore
        except ImportError:
            return 0
        # ВАЖНО: opentele.Storage.ReadFile сам добавляет суффиксы ("s", "1",
        # "0") к имени файла, т.е. для форков читает «key_datass», а для
        # официального TD — «key_datas»/«key_data1». Поэтому перебираем оба
        # базовых имени и ловим исключения на каждой итерации.
        # Дополнительно читаем файлы ПРЯМЫМ открытием (без Storage.ReadFile),
        # чтобы сканер работал даже при несовпадении схемы суффиксов opentele.
        tried: set[str] = set()
        candidates: list[tuple[str, str]] = []
        for key_file_name in ("key_data", "key_datas"):
            for suffix in ("s", "1", "0", ""):
                fname = key_file_name + suffix
                if fname in tried:
                    continue
                tried.add(fname)
                full = os.path.join(base_path, fname)
                if os.path.isfile(full):
                    candidates.append((key_file_name, full))

        for key_file_name, full_path in candidates:
            try:
                with open(full_path, "rb") as _f:
                    raw = _f.read()
                from PySide6.QtCore import QBuffer, QIODevice  # type: ignore

                buf = QBuffer()
                buf.setData(QByteArray(raw))
                buf.open(QIODevice.ReadOnly)
                key_file = td.MTPFile(buf)
            except Exception:  # noqa: BLE001 — файл битый, пробуем дальше
                continue
            try:
                salt, key_encrypted, info_encrypted = QByteArray(), QByteArray(), QByteArray()
                key_file.stream >> salt >> key_encrypted >> info_encrypted
                pass_bytes = (password or "").encode("ascii")
                local = td.Storage.CreateLocalKey(salt, QByteArray(pass_bytes))
                inner = td.Storage.DecryptLocal(key_encrypted, local)
                auth_key = td.AuthKey(inner.stream.readRawData(256))
                info = td.Storage.DecryptLocal(info_encrypted, auth_key)
                count = info.stream.readInt32()
                if count > 0:
                    return int(count)
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    f"Не удалось посчитать аккаунты в {base_path} "
                    f"({key_file_name}): {exc}"
                )
        return 0

    @staticmethod
    def _list_account_subdirs(container: str) -> list[str]:
        """Имена подпапок-аккаунтов контейнера tdata.

        Telethon-структура форков (AyuGram/iGram): внутри tdata лежат
        числовые папки ("1", "2", ...) либо папки с произвольными именами;
        служебные папки вида "mtpusr~", "work", "logs" отбрасываются по
        признаку содержимого (в аккаунт-папке должен быть хотя бы файл
        "map" или "key_datas").
        """
        found: list[str] = []
        try:
            entries = sorted(
                os.listdir(container),
                key=lambda n: (0, int(n)) if n.isdigit() else (1, n.lower()),
            )
        except OSError:
            return found
        for name in entries:
            acc_dir = os.path.join(container, name)
            if not os.path.isdir(acc_dir):
                continue
            has_map = os.path.isfile(os.path.join(acc_dir, "map"))
            has_kds = os.path.isfile(os.path.join(acc_dir, "key_datas"))
            # Папки без признаков аккаунта пропускаем (служебные)
            if has_map or has_kds:
                found.append(name)
        return found

    def find_tdata_accounts(base_path: str) -> list[tuple[str, str]]:
        """
        Поиск аккаунтов tdata внутри указанного пути.

        Поддерживаемые структуры:
        - AyuGram/iGram/форки Unigram: общий файл «key_datas» В КОРНЕ tdata
          + подпапки аккаунтов ("1", "2", "3", "4"); возвращаются все
          аккаунты этого контейнера;
        - старый формат форков: у каждого аккаунта свой «key_datas» внутри
          своей подпапки;
        - официальный / portable Telegram Desktop: файл «key_data» в корне
          tdata (несколько аккаунтов внутри одного файла) — возвращается
          одна запись на каждый аккаунт, если удалось прочитать заголовок.

        :param base_path: Путь к папке tdata ИЛИ к AppData-папке клиента.
        :return: Список пар (отображаемое имя, путь к КОНТЕЙНЕРУ tdata).
                 Выбор конкретного аккаунта выполняется GUI по имени метки
                 («Аккаунт 3» -> индекс 3) при импорте.
        """
        base = os.path.abspath((base_path or "").strip().strip('"'))
        if not os.path.isdir(base):
            return []

        def _has_keys(path: str) -> bool:
            """В папке есть любой файл ключей tdata?"""
            try:
                names = [e.lower() for e in os.listdir(path)]
            except OSError:
                return False
            return any(
                n.startswith("key_data") or n == "data" for n in names
            )

        def _container_candidates(start: str) -> list[str]:
            """Собираем возможные корни контейнеров tdata из выбранного пути."""
            cands: list[str] = []
            norm = os.path.normcase(start)
            # a) сам путь — контейнер tdata (в нём есть key_data*)
            if _has_keys(start):
                cands.append(start)
            # b) рядом лежит ./tdata (выбрана AppData-папка клиента)
            tdata_child = os.path.join(start, "tdata")
            if os.path.isdir(tdata_child) and _has_keys(tdata_child):
                cands.append(tdata_child)
            # c) рядом лежит ../tdata (выбрана подпапка внутри tdata или
            #    профиль portable-клиента)
            sibling = os.path.join(os.path.dirname(start), "tdata")
            if os.path.normcase(sibling) != norm and os.path.isdir(sibling) and _has_keys(sibling):
                cands.append(sibling)
            # c2) поднятие на несколько уровней вверх: пользователь мог
            #     выбрать .../tdata/<аккаунт>/logs и т.п. Ищем ближайшего
            #     предка с именем tdata, содержащего файлы ключей.
            if not cands:
                parent = os.path.dirname(start)
                for _ in range(4):
                    if not parent or parent == os.path.dirname(parent):
                        break
                    if os.path.basename(parent).lower() == "tdata" and _has_keys(parent):
                        cands.append(parent)
                        break
                    parent = os.path.dirname(parent)
            # d) выбрана НЕ tdata, но внутри лежат подпапки с key_datas
            #    (пользователь указал родитель типа .../AyuGramDesktop/tdata
            #    уже обработан выше; этот случай — когда ключи глубже)
            if not cands:
                try:
                    for name in os.listdir(start):
                        sub = os.path.join(start, name)
                        if os.path.isdir(sub) and _has_keys(sub):
                            cands.append(sub)
                except OSError:
                    pass
            # дедупликация
            uniq: dict[str, str] = {}
            for c in cands:
                uniq[os.path.normcase(os.path.abspath(c))] = c
            return list(uniq.values())

        results: list[tuple[str, str]] = []

        for container in _container_candidates(base):
            has_key_datas = os.path.isfile(os.path.join(container, "key_datas"))
            has_key_data = os.path.isfile(os.path.join(container, "key_data"))

            # --- Форки Unigram (AyuGram/iGram): общий key_datas + подпапки ---
            if has_key_datas:
                sub_dirs = ClientManager._list_account_subdirs(container)
                if sub_dirs:
                    for name in sub_dirs:
                        label = f"Аккаунт {name}" if name.isdigit() else name
                        results.append((label, container))
                else:
                    # Подпапок нет — пробуем посчитать аккаунты внутри
                    # общего key_datas; если не вышло — показываем один.
                    n = ClientManager._read_tdata_accounts_count(
                        container, None
                    ) or 1
                    for i in range(1, n + 1):
                        results.append((f"Аккаунт {i}", container))

            # --- Официальный/portable TD: единый key_data со списком ---
            if has_key_data:
                try:
                    n = ClientManager._read_tdata_accounts_count(container, None)
                except Exception:  # noqa: BLE001 — сбой чтения не должен ломать скан
                    n = 0
                if n <= 0:
                    n = 1
                for i in range(1, n + 1):
                    label = (
                        f"Аккаунт {i}"
                        if n > 1
                        else f"Telegram Desktop ({os.path.basename(os.path.dirname(container))})"
                    )
                    results.append((label, container))

        # Дедупликация по (метка, контейнер)
        unique: dict[tuple[str, str], tuple[str, str]] = {}
        for name, path in results:
            unique[(name, os.path.normcase(os.path.abspath(path)))] = (
                name,
                path,
            )
        return sorted(unique.values(), key=lambda item: item[0].lower())

    async def add_account_by_tdata(
        self,
        tdata_path: str,
        password: Optional[str] = None,
        proxy_id: Optional[int] = None,
        on_status: Optional[Callable[[str], None]] = None,
        account_name: Optional[str] = None,
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
        # 1. Зависимости opentele (тяжёлые импорты — только по требованию).
        # ВАЖНО: модуля opentele.tdata НЕ существует ни в одной версии
        # opentele (это была ошибка предыдущей сборки кода — из-за неё импорт
        # падал, корутина завершалась исключением, а GUI мог оставаться на
        # стадии «идёт конвертация»). Публичный API пакета:
        #   opentele.td.TDesktop      — чтение папки tdata;
        #   opentele.tl.TelegramClient — обёртка над Telethon + StringSession.
        try:
            from opentele.td import TDesktop  # type: ignore
            from opentele.tl import TelegramClient as OTelegramClient  # type: ignore
            from telethon.sessions import StringSession as _SS  # noqa: F401
        except ImportError as exc:
            raise ClientManagerError(
                "Для импорта TData необходим пакет opentele.\n\n"
                "Установите его командой:\n"
                "    pip install opentele\n\n"
                "Проверить установку: python -c \"from opentele.td import TDesktop\""
            ) from exc

        # 2. Валидация пути. opentele умеет читать два варианта расположения:
        #    a) корневая папка tdata (...\\Telegram\\tdata) — при импорте из GUI
        #       мы передаём сюда КОНКРЕТНУЮ подпапку аккаунта;
        #    b) сама подпапка аккаунта с key_datas — тоже поддерживается:
        #       родительская папка tdata используется как base для TDesktop,
        #       а нужный аккаунт выбирается по индексу после загрузки.
        abs_path = os.path.abspath((tdata_path or "").strip().strip('"'))
        if not os.path.isdir(abs_path):
            raise ClientManagerError(
                f"Папка не найдена: {abs_path}\n\n"
                "Нужно указать КОРНЕВУЮ папку tdata, например:\n"
                "C:\\Users\\ВашНик\\AppData\\Roaming\\AyuGramDesktop\\tdata"
            )

        def _is_account_dir(path: str) -> bool:
            """Папка конкретного аккаунта tdata (содержит key_datas)."""
            return os.path.isfile(os.path.join(path, "key_datas"))

        if _is_account_dir(abs_path):
            # Указана подпапка аккаунта -> base = родительская папка tdata;
            # имя подпапки используется для выбора аккаунта внутри контейнера
            account_name = account_name or os.path.basename(abs_path)
            abs_path = os.path.dirname(abs_path)

        def _has_key_file(path: str) -> bool:
            """Есть ли в папке файлы ключей tdata (key_data / key_datas)."""
            try:
                entries = os.listdir(path)
            except OSError:
                return False
            names = [e.lower() for e in entries]
            return any(n.startswith("key_data") for n in names)

        # Нормализация пути: если выбрана НЕ корневая папка tdata, ищем
        # контейнер с ключами рядом/внутри (порядок как в find_tdata_accounts).
        if not _has_key_file(abs_path):
            resolved: Optional[str] = None
            # a) ./tdata (выбрана AppData-папка клиента .../AyuGramDesktop)
            child = os.path.join(abs_path, "tdata")
            if os.path.isdir(child) and _has_key_file(child):
                resolved = child
            else:
                # b) ../tdata (выбрана подпапка аккаунта или профиль portable)
                sibling = os.path.join(os.path.dirname(abs_path), "tdata")
                if os.path.normcase(sibling) != os.path.normcase(abs_path) \
                        and os.path.isdir(sibling) and _has_key_file(sibling):
                    resolved = sibling
            if resolved is None:
                # c) глубже: любая непосредственная подпапка с ключами
                try:
                    for name in sorted(os.listdir(abs_path)):
                        sub = os.path.join(abs_path, name)
                        if os.path.isdir(sub) and _has_key_file(sub):
                            resolved = sub
                            break
                except OSError:
                    pass
            if resolved is not None:
                abs_path = resolved

        if not _has_key_file(abs_path):
            raise ClientManagerError(
                f"В выбранной папке нет файлов ключей Telegram Desktop.\n\n"
                f"Проверенный путь:\n    {abs_path}\n\n"
                "Ожидаемая структура — внутри папки tdata лежат файлы\n"
                "  «key_datas» (AyuGram/iGram) или «key_data» (Telegram\n"
                "Desktop/portable) и подпапки аккаунтов «1», «2», ...\n\n"
                "Подсказки:\n"
                "• AyuGram: C:\\Users\\<ник>\\AppData\\Roaming\\AyuGramDesktop\\tdata\n"
                "• Portable: <папка_клиента>\\Telegram\\tdata\n"
                "• Если в клиенте включён Local Passcode — введите пароль\n"
                "  в поле «Пароль tdata»."
            )

        def _status(text: str) -> None:
            """Безопасный вызов колбэка статуса."""
            if on_status is not None:
                try:
                    on_status(text)
                except Exception as cb_exc:  # noqa: BLE001
                    logger.warning(f"Ошибка колбэка статуса TData: {cb_exc}")

        # 3. Чтение tdata и получение auth-ключа (CPU-задача, вне Qt-потока).
        _status("tdata_convert")
        try:
            load_result = await asyncio.wait_for(
                asyncio.to_thread(
                    self._load_tdata_account, TDesktop, abs_path,
                    password or None, account_name,
                ),
                timeout=90.0,
            )
        except asyncio.TimeoutError as exc:
            raise ClientManagerError(
                "Чтение папки tdata заняло слишком много времени (>90 сек).\n\n"
                "Закройте AyuGram/Telegram Desktop (файлы могут быть заняты) "
                "и повторите попытку."
            ) from exc
        except ClientManagerError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Неожиданная ошибка чтения TData из {abs_path}")
            raise ClientManagerError(self._format_tdata_error(exc, bool(password))) from exc

        # Если основной загрузчик не смог прочитать аккаунты (например,
        # структура tdata отличается от ожидаемой opentele) — пробуем
        # собственный низкоуровневый парсер key_datas/key_data. Он читает
        # файлы так же, как Telegram Desktop, и умеет работать с форками
        # (AyuGram/iGram), где opentele спотыкается.
        if load_result is None:
            _status("tdata_convert_fallback")
            try:
                load_result = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._load_tdata_manual, abs_path, password or None,
                        account_name,
                    ),
                    timeout=60.0,
                )
            except asyncio.TimeoutError as exc:
                raise ClientManagerError(
                    "Резервное чтение tdata превысило таймаут (60 сек)."
                ) from exc
            except ClientManagerError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception("Ошибка резервного парсера tdata")
                raise ClientManagerError(
                    self._format_tdata_error(exc, bool(password))
                ) from exc

        user_id: int = load_result["user_id"]
        dc_id: int = load_result["dc_id"]
        auth_key: bytes = load_result["auth_key"]
        api_id: int = load_result["api_id"]
        api_hash: str = load_result["api_hash"]
        accounts_count: int = load_result["accounts_count"]

        _status("tdata_converted")

        # 4. Сборка StringSession из auth-ключа (тот же алгоритм, что и у
        # opentele migrate_to_string): заголовок 0x5B + ip:port + DC id +
        # 192-байтовый auth_key.
        try:
            endpoint = load_result["endpoint"]
            header = b"\x5b"
            addr_bytes = f"{endpoint[0]}:{endpoint[1]}".encode("ascii")
            raw = (
                header
                + len(addr_bytes).to_bytes(1, "little")
                + addr_bytes
                + int(dc_id).to_bytes(4, "little")
                + auth_key[:192]
            )
            session_string = _SS.encode(raw).decode("ascii")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка сборки строковой сессии из TData: {exc}")
            raise ClientManagerError(
                f"Не удалось собрать сессию из прочитанных данных: {exc}"
            ) from exc

        # 5. Сохранение аккаунта в БД (сессия шифруется мастер-ключом).
        # api_id/api_hash аккаунта сохраняются в записи — они понадобятся
        # для подключения (Telethon сверяет ключ сессии с api_id).
        encrypted_session = self.encryption.encrypt(session_string)
        account = Account(
            phone=None,
            username=None,
            session_data=encrypted_session,
            proxy_id=proxy_id,
            status="active",
            last_used=None,
        )
        account_id = await self.db.create_account(account)
        account.id = account_id

        # Служебные настройки: id аккаунта -> api credentials этого tdata
        try:
            await self.db.update_setting(
                f"tg_api_id_acc_{account_id}", str(api_id)
            )
            await self.db.update_setting(
                f"tg_api_hash_acc_{account_id}", api_hash
            )
        except Exception as set_exc:  # noqa: BLE001
            logger.warning(f"Не удалось сохранить API-пары аккаунта: {set_exc}")

        logger.info(
            f"Аккаунт импортирован из TData: id={account_id}, "
            f"user_id={user_id}, dc={dc_id}, всего аккаунтов в tdata: {accounts_count}"
        )

        # 6. Опциональная проверка сессии подключением к Telegram.
        # Не обязательна: даже если сеть недоступна, аккаунт уже сохранён.
        _status("connecting")
        verify_warning: Optional[str] = None
        try:
            me = await self._verify_session_official(
                session_string, api_id, api_hash, proxy_id=proxy_id
            )
            username = getattr(me, "username", None)
            phone = getattr(me, "phone", None)
            if username or phone:
                account.username = username
                account.phone = phone
                await self.db.update_account(account)
        except ClientManagerError as exc:
            verify_warning = str(exc)
            logger.warning(
                f"TData импортирован, но проверка сессии не выполнена: {exc}"
            )
        _status("done")

        if verify_warning:
            # Прикрепляем предупреждение к объекту для отображения в GUI
            account.session_data = account.session_data  # без изменений
            setattr(account, "_verify_warning", verify_warning)
        return account

    # ------------------------------------------------------------------
    # Вспомогательные методы импорта TData
    # ------------------------------------------------------------------
    @staticmethod
    def _tdata_root_for(base_path: str) -> tuple[str, Optional[str]]:
        """
        Определение КОРНЕВОЙ папки tdata и имени подпапки-аккаунта.

        opentele.TDesktop принимает именно корневую папку tdata (в ней лежат
        key_data/key_datas). Если пользователь выбрал подпапку конкретного
        аккаунта форка (...\\tdata\\Work) — поднимаемся на уровень выше.

        :return: (корневая папка tdata, имя подпапки аккаунта или None).
        """
        path = os.path.abspath((base_path or "").strip().strip('"'))

        def _is_tdata_root(p: str) -> bool:
            """В папке p есть файлы ключей tdata (key_data/key_datas)?"""
            return any(
                os.path.isfile(os.path.join(p, f))
                for f in ("key_datas", "key_data")
            )

        # 1) Сам путь — корень tdata (есть key_data*/ключи и подпапки-аккаунты)
        if _is_tdata_root(path):
            return path, None
        # 2) Путь — подпапка аккаунта форка (.../tdata/<акк>/key_datas)
        if os.path.isfile(os.path.join(path, "key_datas")):
            return os.path.dirname(path), os.path.basename(path)
        # 3) Родитель пути — корень tdata (выбрана подпапка «1», «Work» и т.п.)
        parent = os.path.dirname(path)
        if _is_tdata_root(parent):
            return parent, os.path.basename(path)
        # 4) Рядом лежит ./tdata (выбрана AppData-папка клиента)
        sibling = os.path.join(path, "tdata")
        if os.path.isdir(sibling) and _is_tdata_root(sibling):
            return sibling, None
        return path, None

    @staticmethod
    def _load_tdata_account(
        TDesktop_cls: Any,
        base_path: str,
        password: Optional[str],
        account_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Чтение корневой папки tdata и извлечение данных авторизации.

        Выполняется в отдельном потоке (синхронный CPU/disk код opentele).

        :param TDesktop_cls: Класс opentele.td.TDesktop.
        :param base_path: Корневая папка tdata ИЛИ подпапка аккаунта форка
                          (путь нормализуется автоматически).
        :param password: Локальный пароль tdata (passcode), если задан.
        :param account_name: Имя подпапки аккаунта (для выбора конкретного
                             аккаунта, если их несколько в tdata).
        :return: Словарь с полями: user_id, dc_id, auth_key (bytes),
                 api_id, api_hash, endpoint (ip, port), accounts_count,
                 index (индекс выбранного аккаунта в контейнере).
                 Возвращает None, если opentele не смог загрузить аккаунты
                 («No account has been loaded» / «accountsCount is zero») —
                 в этом случае вызывающий код использует резервный парсер.
        """
        root, sub_name = ClientManager._tdata_root_for(base_path)
        target_name = account_name or sub_name

        last_exc: Optional[Exception] = None
        for key_file in ("data", "datas"):
            try:
                tdesk = TDesktop_cls(root, passcode=password, keyFile=key_file)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                text = str(exc).lower()
                if "could not open" in text or "invalid magic" in text \
                        or "not found" in text or "no file" in text:
                    # Файл ключей с таким именем отсутствует — пробуем другой
                    continue
                if "no account has been loaded" in text or "accountcount is zero" in text:
                    logger.warning(
                        f"opentele не загрузил аккаунты из {root} "
                        f"(key_{key_file}) — пробуем альтернативный файл ключей"
                    )
                    continue
                raise
            if not tdesk.isLoaded():
                last_exc = RuntimeError("TDesktop не загрузил данные из папки tdata")
                continue

            accounts = list(tdesk.accounts)
            if not accounts:
                last_exc = RuntimeError("TDesktop вернул 0 аккаунтов")
                continue

            # Выбор конкретного аккаунта по индексу подпапки (форки AyuGram:
            # индекс папки == индекс аккаунта внутри key_datas)
            account = tdesk.mainAccount or accounts[0]
            chosen_index = accounts.index(account) if account in accounts else 0
            if target_name and target_name.isdigit():
                idx = int(target_name) - 1  # имена вида "1", "2" — 1-индексные
                if 0 <= idx < len(accounts):
                    account = accounts[idx]
                    chosen_index = idx
            auth_key_obj = account.authKey
            if auth_key_obj is None:
                raise RuntimeError(
                    "Аккаунт в tdata не авторизован (пустой auth_key). "
                    "Откройте AyuGram и убедитесь, что аккаунт активен."
                )

            # Endpoint ДЦ из конфигурации, сохранённой в tdata
            from opentele.td import MTP  # type: ignore

            endpoints = account._local.config.endpoints(account.MainDcId)
            ipv4_tcp = endpoints[MTP.DcOptions.Address.IPv4][MTP.DcOptions.Protocol.Tcp]
            if not ipv4_tcp:
                raise RuntimeError(
                    "В конфиге tdata не найдено IPv4 TCP-эндпоинтов для ДЦ "
                    f"{int(account.MainDcId)}"
                )
            endpoint = ipv4_tcp[0]

            api_data = account.api
            return {
                "user_id": int(account.UserId),
                "dc_id": int(account.MainDcId),
                "auth_key": bytes(auth_key_obj.key)[:192],
                "api_id": int(getattr(api_data, "api_id", 0)),
                "api_hash": str(getattr(api_data, "api_hash", "") or ""),
                "endpoint": (str(endpoint.ip), int(endpoint.port)),
                "accounts_count": len(accounts),
                "index": chosen_index,
            }

        # Ни один вариант файла ключей не подошёл — сигнал резервному парсеру
        logger.warning(
            f"Не удалось прочитать tdata через opentele ({root}): {last_exc}"
        )
        return None

    @staticmethod
    def _load_tdata_manual(
        base_path: str,
        password: Optional[str],
        account_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Резервное чтение tdata БЕЗ полной загрузки аккаунтов opentele.

        Используется, когда opentele не распознаёт структуру (например,
        файлы форков с нестандартным содержимым). Читает localKey из
        key_data/key_datas и расшифровывает mtpauthorization нужного
        аккаунта напрямую (тот же формат, что у Telegram Desktop).

        :return: Словарь как у _load_tdata_account.
        :raises ClientManagerError: Если прочитать/расшифровать не удалось.
        """
        try:
            from PySide6.QtCore import QByteArray  # type: ignore
            from opentele import td  # type: ignore
        except ImportError as exc:
            raise ClientManagerError(
                "Для импорта TData необходим пакет opentele:\n"
                "    pip install opentele"
            ) from exc

        root, sub_name = ClientManager._tdata_root_for(base_path)
        target_name = account_name or sub_name

        last_err: Optional[str] = None
        for key_file in ("key_datas", "key_data"):
            full = os.path.join(root, key_file)
            if not os.path.isfile(full):
                continue
            try:
                desc = td.Storage.ReadFile(key_file, root)
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                continue
            try:
                salt, key_encrypted, info_encrypted = QByteArray(), QByteArray(), QByteArray()
                desc.stream >> salt >> key_encrypted >> info_encrypted
                pass_bytes = (password or "").encode("ascii")
                local = td.Storage.CreateLocalKey(salt, QByteArray(pass_bytes))
                inner = td.Storage.DecryptLocal(key_encrypted, local)
                auth_key_obj = td.AuthKey(inner.stream.readRawData(256))
                info = td.Storage.DecryptLocal(info_encrypted, auth_key_obj)
                count = info.stream.readInt32()
                indexes = [info.stream.readInt32() for _ in range(max(0, count))]
            except Exception as exc:  # noqa: BLE001
                text = str(exc).lower()
                if "decrypt" in text or "bad" in text or "magic" in text:
                    raise ClientManagerError(
                        "Не удалось расшифровать tdata. Если в клиенте включён "
                        "локальный пароль (Local Passcode) — введите его в поле "
                        "«Пароль tdata»."
                    ) from exc
                last_err = str(exc)
                continue

            if not indexes:
                last_err = "список аккаунтов пуст"
                continue

            # Индекс целевого аккаунта: имя подпапки ("2" -> индекс 1)
            target_index = indexes[0]
            if target_name and str(target_name).isdigit():
                want = int(target_name) - 1
                if want in indexes:
                    target_index = want

            try:
                acc_desc = td.Storage.ReadFile(f"{target_index}", root)
                serialized = td.Storage.DecryptLocal(acc_desc.encrypted, auth_key_obj)
            except ClientManagerError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise ClientManagerError(
                    f"Не удалось прочитать данные аккаунта №{target_index + 1} "
                    f"из tdata: {exc}"
                ) from exc

            parsed = ClientManager._parse_mtp_authorization(serialized.data())
            if parsed is None:
                last_err = "не удалось разобрать mtpauthorization"
                continue
            parsed["accounts_count"] = len(indexes)
            parsed["index"] = target_index
            return parsed

        raise ClientManagerError(
            "Резервный парсер не смог прочитать tdata.\n\n"
            f"Причина: {last_err or 'файлы ключей не найдены'}\n\n"
            "Проверьте, что указана папка tdata (а не её родитель), и что "
            "клиент полностью закрыт."
        )

    @staticmethod
    def _parse_mtp_authorization(raw: bytes) -> Optional[dict[str, Any]]:
        """
        Разбор сериализованной структуры mtpauthorization Telegram Desktop.

        Формат (Qt QDataStream, little endian):
          int32 user_id | int32 dc_id | (расширенный 64-бит заголовок опц.)
          ... список auth-ключей DC ...

        :param raw: Расшифрованные байты блока аккаунта.
        :return: Словарь с user_id/dc_id/auth_key или None при ошибке.
        """
        import struct as _struct

        if len(raw) < 8:
            return None
        try:
            pos = 0

            def i32() -> int:
                nonlocal pos
                val = _struct.unpack_from("<i", raw, pos)[0]
                pos += 4
                return val

            def i64() -> int:
                nonlocal pos
                val = _struct.unpack_from("<q", raw, pos)[0]
                pos += 8
                return val

            user_id = i32()
            dc_id = i32()
            if ((user_id << 32) | (dc_id & 0xFFFFFFFF)) == -2:
                # Признак широкого формата id — перечитываем
                pos = 0
                _ = i32()
                _ = i32()
                user_id = i64()
                dc_id = i32()

            # Поиск 256-байтового auth-ключа основного ДЦ: структура ключа
            # в TD сериализуется как [len(int32)][key bytes][dcId][userId]...
            # Надёжнее всего перебрать блоки длиной 256.
            auth_key: Optional[bytes] = None
            while pos + 4 <= len(raw):
                size = i32()
                if size == 256 and pos + 256 <= len(raw):
                    candidate = raw[pos:pos + 256]
                    pos += 256
                    # После ключа идут int32 dc_id и int64/uint32 user_id
                    try:
                        k_dc = i32()
                        k_user = i32()
                        if ((k_user << 32) | (k_dc & 0xFFFFFFFF)) == -2:
                            pos -= 4
                            k_user = i64()
                            k_dc = i32()
                    except Exception:
                        break
                    if k_dc == dc_id and (k_user == user_id or k_user == 0):
                        auth_key = candidate
                        break
                    pos -= 8
                    continue
                pos += max(0, size)
            if auth_key is None:
                return None
            return {
                "user_id": int(user_id),
                "dc_id": int(dc_id),
                "auth_key": auth_key[:192],
                "api_id": 0,
                "api_hash": "",
                "endpoint": ("", 0),
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Ошибка разбора mtpauthorization: {exc}")
            return None

    @staticmethod
    def _format_tdata_error(exc: Exception, had_password: bool) -> str:
        """
        Преобразование ошибок opentele при чтении tdata в понятный текст.

        :param exc: Исключение, возникшее при загрузке TDesktop.
        :param had_password: Был ли передан пароль tdata.
        :return: Человекочитаемое сообщение (рус.).
        """
        text = str(exc)
        lower = text.lower()
        if "decrypt" in lower or "passcode" in lower or "bad" in lower and "key" in lower:
            if had_password:
                return (
                    "Неверный локальный пароль tdata. Введите правильный пароль "
                    "в поле «Пароль tdata»."
                )
            return (
                "Папка tdata защищена локальным паролем (Local Passcode в "
                "AyuGram/Telegram Desktop). Введите его в поле «Пароль tdata»."
            )
        if "not loaded" in lower or "no account" in lower or "zero" in lower:
            return (
                f"Не удалось прочитать аккаунты из tdata: {text}\n\n"
                "Убедитесь, что указана КОРНЕВАЯ папка tdata "
                "(...\\AyuGramDesktop\\tdata) и что AyuGram полностью закрыт."
            )
        return (
            f"Ошибка чтения TData: {text}\n\n"
            "Советы:\n"
            "1) Полностью закройте AyuGram/Telegram Desktop (правый клик в трее "
            "→ Выход) — файлы tdata могут быть заняты;\n"
            "2) Указывайте корневую папку tdata, а не подпапку аккаунта;\n"
            "3) Если в клиенте задан Local Passcode — введите его в диалоге."
        )

    async def _verify_session_official(
        self,
        session_string: str,
        api_id: int,
        api_hash: str,
        proxy_id: Optional[int] = None,
    ) -> Any:
        """
        Проверка собранной строковой сессии подключением к Telegram.

        Использует api_id/api_hash, извлечённые из того же tdata (ключ сессии
        жёстко привязан к api_id, поэтому сторонние ключи не подойдут).

        :return: Объект me (User) из client.get_me().
        :raises ClientManagerError: При сетевых ошибках/невалидной сессии.
        """
        proxy_cfg = (
            await self.build_proxy_config(int(proxy_id))
            if proxy_id is not None
            else None
        )

        async def _get_me(client: TelegramClient) -> Any:
            """Запрос данных пользователя."""
            me = await client.get_me()
            if me is None:
                raise ClientManagerError(
                    "Сессия не авторизована: Telegram не вернул данные "
                    "пользователя."
                )
            return me

        try:
            return await self._run_temp_client_with_api(
                StringSession(session_string),
                proxy_cfg,
                _get_me,
                api_id=api_id,
                api_hash=api_hash,
                connect_timeout=25.0,
                request_timeout=25.0,
            )
        except ClientManagerError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка проверки TData-сессии: {exc}")
            raise ClientManagerError(format_telethon_error(exc)) from exc

    async def _run_temp_client_with_api(
        self,
        session: "StringSession",
        proxy_cfg: Optional[dict[str, Any]],
        action: Callable[[TelegramClient], Coroutine[Any, Any, Any]],
        api_id: int,
        api_hash: str,
        connect_timeout: float = 25.0,
        request_timeout: float = 35.0,
    ) -> Any:
        """
        То же, что _run_temp_client, но с ЯВНЫМИ api_id/api_hash
        (для сессий, импортированных из чужого tdata).
        """
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
            await asyncio.wait_for(client.connect(), timeout=connect_timeout)
            return await asyncio.wait_for(action(client), timeout=request_timeout)
        except asyncio.TimeoutError as exc:
            raise ClientManagerError(
                "Нет соединения с серверами Telegram (таймаут) — аккаунт "
                "сохранён в базе, но проверить сессию не удалось. Попробуйте "
                "кнопку «Проверить» во вкладке Аккаунты позже или используйте "
                "прокси."
            ) from exc
        finally:
            if client is not None:
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=10.0)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Ошибка отключения временного клиента: {exc}")

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
