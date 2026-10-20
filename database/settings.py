# -*- coding: utf-8 -*-
"""
Менеджер глобальных настроек приложения (таблица settings).

При первом запуске инициализирует все ключи значениями по умолчанию,
заданными в ТЗ v2.0.
"""

from typing import Optional

from loguru import logger

from database.db import DatabaseManager

# Ключи настроек
KEY_ACTIVITY_THRESHOLD_DAYS = "activity_threshold_days"
KEY_DEFAULT_DELAY_MIN = "default_delay_min"
KEY_DEFAULT_DELAY_MAX = "default_delay_max"
KEY_MAX_CONCURRENT_TASKS = "max_concurrent_tasks"
KEY_MAX_RETRIES = "max_retries"
KEY_EXPORT_DIR = "export_dir"

# Значения по умолчанию согласно ТЗ
DEFAULT_SETTINGS: dict[str, str] = {
    KEY_ACTIVITY_THRESHOLD_DAYS: "7",      # дней активности канала
    KEY_DEFAULT_DELAY_MIN: "2.0",          # минимальная задержка, сек
    KEY_DEFAULT_DELAY_MAX: "6.0",          # максимальная задержка, сек
    KEY_MAX_CONCURRENT_TASKS: "3",         # максимум параллельных задач
    KEY_MAX_RETRIES: "3",                  # количество повторов при ошибках
    KEY_EXPORT_DIR: "./exports",           # каталог экспорта
}


class SettingsManager:
    """Доступ к настройкам приложения с типизированными геттерами/сеттерами."""

    def __init__(self, db: DatabaseManager) -> None:
        """
        :param db: Инициализированный менеджер базы данных.
        """
        self.db: DatabaseManager = db

    # ------------------------------------------------------------------
    # Инициализация значений по умолчанию
    # ------------------------------------------------------------------
    async def initialize_defaults(self) -> None:
        """Запись отсутствующих настроек значениями по умолчанию (первый запуск)."""
        try:
            existing = await self.db.get_settings()
            for key, default_value in DEFAULT_SETTINGS.items():
                if key not in existing:
                    await self.db.update_setting(key, default_value)
                    logger.debug(f"Настройка '{key}' инициализирована значением {default_value}")
            logger.info("Настройки по умолчанию проверены/инициализированы")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка инициализации настроек по умолчанию: {exc}")
            raise

    # ------------------------------------------------------------------
    # Универсальные методы чтения/записи
    # ------------------------------------------------------------------
    async def _get_value(self, key: str, default: str) -> str:
        """Чтение строкового значения настройки с фолбэком на дефолт."""
        try:
            value: Optional[str] = await self.db.get_setting(key)
            return value if value is not None else default
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка чтения настройки '{key}': {exc}")
            return default

    async def _set_value(self, key: str, value: str) -> None:
        """Запись строкового значения настройки."""
        try:
            await self.db.update_setting(key, value)
            logger.debug(f"Настройка '{key}' обновлена: {value}")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка записи настройки '{key}': {exc}")
            raise

    # ------------------------------------------------------------------
    # Геттеры
    # ------------------------------------------------------------------
    async def get_activity_threshold_days(self) -> int:
        """Порог активности канала в днях (default: 7)."""
        raw = await self._get_value(KEY_ACTIVITY_THRESHOLD_DAYS,
                                    DEFAULT_SETTINGS[KEY_ACTIVITY_THRESHOLD_DAYS])
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            logger.warning(f"Некорректное значение '{raw}', использован default")
            return int(DEFAULT_SETTINGS[KEY_ACTIVITY_THRESHOLD_DAYS])

    async def get_default_delay_min(self) -> float:
        """Минимальная задержка между запросами, сек (default: 2.0)."""
        raw = await self._get_value(KEY_DEFAULT_DELAY_MIN,
                                    DEFAULT_SETTINGS[KEY_DEFAULT_DELAY_MIN])
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(DEFAULT_SETTINGS[KEY_DEFAULT_DELAY_MIN])

    async def get_default_delay_max(self) -> float:
        """Максимальная задержка между запросами, сек (default: 6.0)."""
        raw = await self._get_value(KEY_DEFAULT_DELAY_MAX,
                                    DEFAULT_SETTINGS[KEY_DEFAULT_DELAY_MAX])
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(DEFAULT_SETTINGS[KEY_DEFAULT_DELAY_MAX])

    async def get_max_concurrent_tasks(self) -> int:
        """Максимум одновременно выполняемых задач (default: 3)."""
        raw = await self._get_value(KEY_MAX_CONCURRENT_TASKS,
                                    DEFAULT_SETTINGS[KEY_MAX_CONCURRENT_TASKS])
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            return int(DEFAULT_SETTINGS[KEY_MAX_CONCURRENT_TASKS])

    async def get_max_retries(self) -> int:
        """Максимальное число повторов операции (default: 3)."""
        raw = await self._get_value(KEY_MAX_RETRIES,
                                    DEFAULT_SETTINGS[KEY_MAX_RETRIES])
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            return int(DEFAULT_SETTINGS[KEY_MAX_RETRIES])

    async def get_export_dir(self) -> str:
        """Каталог для файлов экспорта (default: ./exports)."""
        return await self._get_value(KEY_EXPORT_DIR,
                                     DEFAULT_SETTINGS[KEY_EXPORT_DIR])

    # ------------------------------------------------------------------
    # Сеттеры
    # ------------------------------------------------------------------
    async def set_activity_threshold_days(self, value: int) -> None:
        """Установка порога активности канала в днях."""
        await self._set_value(KEY_ACTIVITY_THRESHOLD_DAYS, str(int(value)))

    async def set_default_delay_min(self, value: float) -> None:
        """Установка минимальной задержки между запросами, сек."""
        await self._set_value(KEY_DEFAULT_DELAY_MIN, str(float(value)))

    async def set_default_delay_max(self, value: float) -> None:
        """Установка максимальной задержки между запросами, сек."""
        await self._set_value(KEY_DEFAULT_DELAY_MAX, str(float(value)))

    async def set_max_concurrent_tasks(self, value: int) -> None:
        """Установка максимума параллельных задач."""
        await self._set_value(KEY_MAX_CONCURRENT_TASKS, str(int(value)))

    async def set_max_retries(self, value: int) -> None:
        """Установка максимального числа повторов."""
        await self._set_value(KEY_MAX_RETRIES, str(int(value)))

    async def set_export_dir(self, value: str) -> None:
        """Установка каталога экспорта."""
        await self._set_value(KEY_EXPORT_DIR, str(value))
