# -*- coding: utf-8 -*-
"""
Точка входа приложения Telegram Discovery.

Схема работы (production-подход для PySide6 + aiosqlite):
- Qt event loop крутится в главном потоке;
- отдельный asyncio event loop работает в фоновом потоке (AsyncBridge);
- все корутины (обращения к БД, фоновые задачи) планируются в этот loop
  через asyncio.run_coroutine_threadsafe;
- результаты возвращаются в GUI через Qt-сигналы (потокобезопасно).
"""

import asyncio
import sys
import threading
from concurrent.futures import Future
from typing import Any, Callable, Coroutine, Optional

from loguru import logger
from PySide6.QtCore import QObject, QTimer, Signal, Slot
from PySide6.QtWidgets import QApplication

from database.db import DatabaseManager
from database.settings import SettingsManager
from gui.main_window import MainWindow
from utils.logger import setup_logger


class AsyncBridge(QObject):
    """
    Мост между Qt-main-loop (главный поток) и asyncio loop (фоновый поток).

    Сигналы bridge используются для потокобезопасной доставки
    результатов асинхронных операций в GUI.
    """

    initialized = Signal()                 # БД и настройки готовы
    initialization_failed = Signal(str)    # текст ошибки инициализации
    operation_failed = Signal(str)         # ошибка фоновой операции

    def __init__(self) -> None:
        super().__init__()
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self.db: Optional[DatabaseManager] = None
        self.settings: Optional[SettingsManager] = None

    # ------------------------------------------------------------------
    # Управление фоновым loop
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Запуск фонового потока с собственным asyncio event loop."""
        self._thread = threading.Thread(
            target=self._thread_main, name="AsyncIO-Bridge", daemon=True
        )
        self._thread.start()

    def _thread_main(self) -> None:
        """Функция фонового потока: создание loop и запуск инициализации БД."""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self._ready.set()
            self.loop.create_task(self._initialize())
            self.loop.run_forever()
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка фонового asyncio-потока: {exc}")
            self.initialization_failed.emit(str(exc))

    async def _initialize(self) -> None:
        """Инициализация БД и настроек по умолчанию внутри asyncio loop."""
        try:
            self.db = DatabaseManager()
            await self.db.initialize()
            self.settings = SettingsManager(self.db)
            await self.settings.initialize_defaults()
            self.initialized.emit()
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка инициализации подсистем: {exc}")
            self.initialization_failed.emit(str(exc))

    def wait_started(self, timeout: float = 10.0) -> bool:
        """Ожидание запуска фоновой петли."""
        return self._ready.wait(timeout)

    def stop(self) -> None:
        """Остановка фонового loop и закрытие соединений с БД."""
        if self.loop is not None and self.loop.is_running():
            async def _shutdown() -> None:
                if self.db is not None:
                    await self.db.close()

            future = self.submit(_shutdown())
            try:
                future.result(timeout=5)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка завершения работы с БД: {exc}")
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Планирование задач из Qt-потока
    # ------------------------------------------------------------------
    def submit(self, coro: Coroutine[Any, Any, Any]) -> Future:
        """
        Планирование корутины в фоновом asyncio loop.

        :param coro: Корутина для выполнения.
        :return: concurrent.futures.Future с результатом.
        """
        if self.loop is None or not self.loop.is_running():
            raise RuntimeError("Asyncio loop не запущен")
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def submit_with_callback(
        self,
        coro_factory: Callable[[], Coroutine[Any, Any, Any]],
        on_success: Optional[Callable[[Any], None]] = None,
    ) -> None:
        """
        Планирование корутины с вызовом колбэка в Qt-потоке по завершении.

        :param coro_factory: Фабрика корутины (вызывается в Qt-потоке).
        :param on_success: Колбэк, получающий результат (вызов в Qt-потоке).
        """
        try:
            future = self.submit(coro_factory())
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка планирования задачи: {exc}")
            self.operation_failed.emit(str(exc))
            return

        def _done(fut: Future) -> None:
            """Колбэк завершения — выполняется в фоновом asyncio-потоке."""
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка выполнения фоновой задачи: {exc}")
                self.operation_failed.emit(str(exc))
                return
            if on_success is not None:
                # singleShot(0) — безопасный перенос колбэка в Qt-поток
                QTimer.singleShot(0, lambda: on_success(result))

        future.add_done_callback(_done)


def main() -> int:
    """Синхронная точка входа: сборка Qt + asyncio моста."""
    # Настройка логирования до создания QApplication
    setup_logger()
    logger.info("Запуск Telegram Discovery...")

    app = QApplication(sys.argv)
    app.setApplicationName("Telegram Discovery")
    app.setApplicationVersion("2.0")

    # Фоновый asyncio loop с инициализацией БД
    bridge = AsyncBridge()
    bridge.initialized.connect(
        lambda: logger.info("База данных и настройки инициализированы")
    )
    bridge.initialization_failed.connect(
        lambda err: logger.error(f"Инициализация не удалась: {err}")
    )
    bridge.operation_failed.connect(
        lambda err: logger.error(f"Ошибка фоновой операции: {err}")
    )
    bridge.start()
    if not bridge.wait_started(timeout=10.0):
        logger.error("Не удалось запустить фоновый asyncio-поток")
        return 1

    window = MainWindow(bridge.db)
    window.show()
    logger.info("Главное окно отображено")

    exit_code = app.exec()

    # Корректное завершение: закрываем БД и останавливаем фоновый loop
    bridge.stop()
    logger.info("Приложение завершено")
    return exit_code


if __name__ == "__main__":
    # На Windows проакторная политика событийного цикла
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    sys.exit(main())
