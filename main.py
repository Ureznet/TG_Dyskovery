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
import os
import sys
import threading
from concurrent.futures import Future
from typing import Any, Callable, Coroutine, Optional

from loguru import logger
from PySide6.QtCore import QObject, QTimer, Signal
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

# Логирование (loguru) используется в модулях приложения; импорт здесь —
# для единообразной инициализации при старте.
from database.db import DatabaseManager
from database.settings import SettingsManager
from gui.main_window import MainWindow
from telegram_client.activity_checker import ActivityChecker
from telegram_client.client_manager import ClientManager
from utils.encryption import EncryptionManager
from utils.logger import setup_logger

# Путь к файлу мастер-ключа
MASTER_KEY_PATH = "data/master.key"


class MasterPasswordDialog(QDialog):
    """Диалог ввода мастер-пароля для шифрования сессий аккаунтов."""

    def __init__(self, title: str, confirm: bool = False,
                 error_text: str = "") -> None:
        """
        :param title: Заголовок диалога.
        :param confirm: Требовать повторный ввод пароля (первый запуск).
        :param error_text: Текст ошибки для повторного запроса.
        """
        super().__init__()
        self.setWindowTitle("Telegram Discovery — Мастер-пароль")
        self.setModal(True)
        self.setMinimumWidth(380)

        layout = QVBoxLayout(self)
        hint = QLabel(title)
        hint.setWordWrap(True)
        layout.addWidget(hint)

        if error_text:
            err = QLabel(error_text)
            err.setStyleSheet("color: #cc0000;")
            err.setWordWrap(True)
            layout.addWidget(err)

        self.edit = QLineEdit()
        self.edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.edit.setPlaceholderText("Мастер-пароль")
        layout.addWidget(self.edit)

        self.edit_confirm: Optional[QLineEdit] = None
        if confirm:
            self.edit_confirm = QLineEdit()
            self.edit_confirm.setEchoMode(QLineEdit.EchoMode.Password)
            self.edit_confirm.setPlaceholderText("Повторите пароль")
            layout.addWidget(self.edit_confirm)

        buttons = QHBoxLayout()
        btn_ok = QPushButton("OK")
        btn_cancel = QPushButton("Отмена")
        buttons.addStretch(1)
        buttons.addWidget(btn_ok)
        buttons.addWidget(btn_cancel)
        layout.addLayout(buttons)

        btn_ok.clicked.connect(self._on_accept)
        btn_cancel.clicked.connect(self.reject)

    def _on_accept(self) -> None:
        """Проверка заполнения и совпадения паролей."""
        if not self.edit.text():
            return
        if self.edit_confirm is not None and \
                self.edit.text() != self.edit_confirm.text():
            QMessageBox.warning(self, "Несовпадение", "Пароли не совпадают.")
            return
        self.accept()

    def password(self) -> str:
        """Введённый мастер-пароль."""
        return self.edit.text()


def ensure_encryption() -> Optional[EncryptionManager]:
    """
    Инициализация менеджера шифрования.

    При первом запуске (нет data/master.key) запрашивает новый мастер-пароль
    и сохраняет ключ. При последующих запусках запрашивает пароль и проверяет
    его по сохранённому ключу.

    :return: Готовый EncryptionManager либо None, если пользователь отказался.
    """
    first_run = not os.path.exists(MASTER_KEY_PATH)

    while True:
        dialog = MasterPasswordDialog(
            title=(
                "Первый запуск: придумайте мастер-пароль для шифрования "
                "сессий Telegram. ЗАПОМИНАНИЕ ОБЯЗАТЕЛЬНО — без него данные "
                "не восстановить."
                if first_run else
                "Введите мастер-пароль для разблокировки хранилища сессий."
            ),
            confirm=first_run,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            logger.warning("Пользователь отменил ввод мастер-пароля")
            return None

        password = dialog.password()
        if first_run:
            encryption = EncryptionManager(password)
            try:
                encryption.save_master_key(MASTER_KEY_PATH)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка сохранения мастер-ключа: {exc}")
                QMessageBox.critical(None, "Ошибка", f"Не удалось сохранить ключ: {exc}")
                return None
            logger.info("Создан новый мастер-ключ")
            return encryption

        # Не первый запуск: проверка пароля по файлу ключа
        encryption = EncryptionManager(password)  # временный вывод ключа
        if encryption.load_master_key(MASTER_KEY_PATH, password):
            return encryption

        reply = QMessageBox.question(
            None, "Неверный пароль",
            "Мастер-пароль неверен. Попробовать ещё раз?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return None


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

    # Инициализация шифрования (мастер-пароль при первом запуске)
    encryption = ensure_encryption()
    if encryption is None:
        logger.warning("Шифрование не инициализировано — выход из приложения")
        return 1

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
    if bridge.db is None:
        logger.error("База данных не инициализирована")
        return 1

    # Менеджер Telethon-клиентов
    client_manager = ClientManager(bridge.db, encryption)
    logger.info("ClientManager инициализирован")

    # Чекер активности каналов (использует тот же мост к БД/настройкам)
    if bridge.settings is None:  # защита типов для статических анализаторов
        logger.error("SettingsManager не инициализирован")
        return 1
    activity_checker = ActivityChecker(client_manager, bridge.db, bridge.settings)
    logger.info("ActivityChecker инициализирован")

    # Порог активности по умолчанию из настроек — для спинбокса вкладки «Чекер»
    threshold_days = 7
    try:
        threshold_days = bridge.submit(
            bridge.settings.get_activity_threshold_days()
        ).result(timeout=5)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Не удалось прочитать порог активности из настроек: {exc}")

    window = MainWindow(
        bridge.db,
        client_manager,
        activity_checker=activity_checker,
        threshold_days_default=int(threshold_days),
        # Вкладка «Аккаунты» планирует задачи в общем asyncio-потоке:
        # собственный QThread-воркер больше не нужен и именно он был
        # источником «зависаний» отправки кода.
        submitter=bridge.submit,
    )
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
