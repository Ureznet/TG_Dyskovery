# -*- coding: utf-8 -*-
"""
Модуль настройки логирования на базе loguru.

Особенности:
- ротация логов каждые 30 дней (``rotation="30 days"``);
- запись в файл logs/app.log и вывод в stderr;
- фильтрация чувствительных данных (номера телефонов, пароли, сессии) —
  значения заменяются на ``***`` (реализовано через loguru patcher).
"""

import os
import re
import sys
from typing import Any

from loguru import logger

# Формат логов согласно ТЗ
LOG_FORMAT: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
    "{module}:{function}:{line} - {extra[safe_message]}"
)

# Паттерны чувствительных данных.
# Каждый кортеж: (скомпилированный regex, имя группы со значением для маскировки)
_SECRETS_KEYWORDS = r"(?:session(?:[ _]?data|[ _]?string)?|password|пароль|token|api[_ ]?hash|api[_ ]?id)"

_SENSITIVE_PATTERNS: list[re.Pattern] = [
    # session_data = "..." / password: '...' — quoted-значения
    re.compile(
        rf"(?P<head>{_SECRETS_KEYWORDS}[=:]\s*[\"'])(?P<value>[^\"']*)(?P<tail>[\"'])",
        re.IGNORECASE,
    ),
    # Ключевые слова с присваиванием: password=secret / session_data: xyz
    # (допускается пробел до разделителя, например "api_hash = value")
    re.compile(rf"(?P<head>{_SECRETS_KEYWORDS}\s*[=:]\s*)(?P<value>[^\s,;\"']+)", re.IGNORECASE),
    # Телефонные номера вида +79991234567, +7 (999) 123-45-67
    re.compile(r"(?P<value>\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})"),
    # Явный ключ phone: <номер>
    re.compile(r"(?P<head>(?:phone|телефон)[=:]?\s*)(?P<value>\+?[\d][\d\s\-()]{6,20})", re.IGNORECASE),
]


def _sanitize(text: str) -> str:
    """
    Замена чувствительных фрагментов текста на ``***``.

    :param text: Исходная строка сообщения лога.
    :return: Строка с замаскированными данными.
    """
    result = text
    for pattern in _SENSITIVE_PATTERNS:
        parts: list[str] = []
        pos = 0
        for m in pattern.finditer(result):
            head = m.groupdict().get("head") or ""
            tail = m.groupdict().get("tail") or ""
            parts.append(result[pos:m.start()])
            parts.append(head)
            parts.append("***")
            parts.append(tail)
            pos = m.end()
        parts.append(result[pos:])
        result = "".join(parts)
    return result


def _masking_patcher(record: dict) -> None:
    """
    Patcher loguru: подготавливает безопасную версию сообщения
    в поле record["extra"]["safe_message"].

    :param record: Запись лога (dict).
    """
    try:
        record["extra"]["safe_message"] = _sanitize(str(record["message"]))
    except Exception:  # noqa: BLE001 - патчер не должен ронять логирование
        record["extra"]["safe_message"] = "<logging error>"


def _safe_format(record: dict) -> str:
    """
    Функция-форматтер для sinks: гарантирует наличие safe_message даже
    для записей, созданных без patcher (например, до вызова setup_logger
    или из сторонних модулей с локальным экземпляром logger).

    :param record: Запись лога (dict).
    :return: Строка формата loguru.
    """
    if "safe_message" not in record["extra"]:
        record["extra"]["safe_message"] = _sanitize(str(record["message"]))
    return LOG_FORMAT


def setup_logger(log_dir: str = "logs", log_level: str = "INFO") -> "logger":
    """
    Настройка loguru: файл app.log с ротацией каждые 30 дней + stderr,
    фильтрация секретов через patcher.

    :param log_dir: Каталог для файлов логов.
    :param log_level: Минимальный уровень логирования.
    :return: Настроенный logger loguru.
    """
    try:
        os.makedirs(log_dir, exist_ok=True)

        # Удаляем стандартный обработчик loguru
        logger.remove()

        log_file = os.path.join(log_dir, "app.log")

        # Файловый sink с ротацией каждые 30 дней
        logger.add(
            log_file,
            level=log_level,
            format=_safe_format,
            rotation="30 days",
            retention="12 months",
            encoding="utf-8",
            enqueue=False,
        )

        # Вывод в stderr
        logger.add(
            sys.stderr,
            level=log_level,
            format=_safe_format,
        )

        # Глобальный патчер — маскирование чувствительных данных.
        # patch() возвращает новый объект-обёртку (Logger), а глобальный
        # singleton logger продолжает писать в те же sinks; чтобы патчер
        # применялся ко всем вызовам (в т.ч. logger.info из других модулей),
        # присваиваем результат в атрибуты модуля loguru.
        patched = logger.patch(_masking_patcher)
        import loguru
        loguru.logger = patched
        globals()["logger"] = patched

        patched.info(f"Логирование настроено: файл {log_file}, уровень {log_level}")
    except Exception as exc:  # noqa: BLE001
        # Fallback: если не удалось создать файл — оставляем только stderr
        try:
            logger.remove()
            logger.add(sys.stderr, level=log_level, format="{message}")
        except Exception:  # noqa: BLE001
            pass
        logger.error(f"Не удалось настроить файловый лог: {exc}")

    return logger
