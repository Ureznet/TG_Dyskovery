# -*- coding: utf-8 -*-
"""
Главное окно приложения Telegram Discovery (PySide6 / Qt 6).

Содержит:
- верхнюю панель инструментов (QToolBar) с кнопками и индикатором статуса;
- центральную область QTabWidget с вкладками Dashboard/Чекер/Аккаунты/Настройки;
- нижнюю панель логов (QTextEdit, read-only) с фильтром по уровню (QComboBox);
- сигналы для интеграции с фоновыми asyncio-задачами.
"""

from datetime import datetime
from typing import Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont, QTextCharFormat, QTextCursor
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from database.db import DatabaseManager
from gui.accounts_window import AccountsWindow
from gui.checker_tab import CheckerTab
from telegram_client.activity_checker import ActivityChecker
from telegram_client.client_manager import ClientManager

# Цвета уровней в панели логов
_LEVEL_COLORS: dict[str, str] = {
    "INFO": "#000000",      # чёрный
    "WARNING": "#e08000",   # оранжевый
    "ERROR": "#cc0000",     # красный
}


class MainWindow(QMainWindow):
    """Главное окно приложения."""

    # Сигналы для связи с фоновыми задачами (пока заглушки)
    task_progress_updated = Signal(int, int)   # (task_id, progress_percent)
    task_completed = Signal(int)               # (task_id)
    log_received = Signal(str, str)            # (level, message)

    def __init__(
        self,
        db: DatabaseManager,
        client_manager: Optional[ClientManager] = None,
        activity_checker: Optional[ActivityChecker] = None,
        threshold_days_default: int = 7,
    ) -> None:
        """
        :param db: Инициализированный менеджер базы данных.
        :param client_manager: Менеджер Telethon-клиентов (для вкладки «Аккаунты»).
        :param activity_checker: Чекер активности (для вкладки «Чекер»).
        :param threshold_days_default: Порог активности по умолчанию из настроек.
        """
        super().__init__()
        self.db: DatabaseManager = db
        self.client_manager: Optional[ClientManager] = client_manager
        self.activity_checker: Optional[ActivityChecker] = activity_checker
        self.threshold_days_default: int = threshold_days_default

        # Вкладка «Чекер» (создаётся в _create_central_widget)
        self.checker_tab: Optional[CheckerTab] = None

        # Счётчики для индикатора статуса
        self._active_tasks_count: int = 0
        self._active_accounts_count: int = 0

        self.setWindowTitle("Telegram Discovery v2.0")
        self.resize(1100, 750)

        self._create_toolbar()
        self._create_central_widget()

        # Внутренний сигнал логирования -> слот панели логов
        self.log_received.connect(self.log_message)

        self.log_message("INFO", "Приложение запущено")

    # ------------------------------------------------------------------
    # Верхняя панель инструментов
    # ------------------------------------------------------------------
    def _create_toolbar(self) -> None:
        """Создание QToolBar с кнопками и индикатором статуса."""
        toolbar = QToolBar("MainToolbar")
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        self.addToolBar(Qt.ToolBarArea.TopToolBarArea, toolbar)

        btn_new_task = QPushButton("Новая задача")
        btn_accounts = QPushButton("Аккаунты")
        btn_settings = QPushButton("Настройки")

        for btn in (btn_new_task, btn_accounts, btn_settings):
            toolbar.addWidget(btn)

        # Индикатор активных задач/аккаунтов
        self.status_indicator = QLabel("Активных задач: 0 | Активных аккаунтов: 0")
        self.status_indicator.setObjectName("statusIndicator")
        self.status_indicator.setStyleSheet(
            "padding: 4px 10px; font-weight: bold; color: #336;"
        )
        # Прижимаем индикатор к правому краю панели
        spacer = QWidget()
        spacer.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred
        )
        toolbar.addWidget(spacer)
        toolbar.addWidget(self.status_indicator)

        # Подключение кнопок к переключению вкладок
        btn_new_task.clicked.connect(lambda: self.tabs.setCurrentIndex(0))
        btn_accounts.clicked.connect(lambda: self.tabs.setCurrentIndex(2))
        btn_settings.clicked.connect(lambda: self.tabs.setCurrentIndex(3))

    # ------------------------------------------------------------------
    # Центральная область: вкладки + панель логов
    # ------------------------------------------------------------------
    def _create_central_widget(self) -> None:
        """Создание QTabWidget с 4 вкладками и нижней панели логов."""
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(6, 6, 6, 6)

        splitter = QSplitter(Qt.Orientation.Vertical)

        # --- Вкладки ---
        self.tabs = QTabWidget()
        self.tabs.addTab(self._create_placeholder_tab(
            "Dashboard", "Выберите задачу или создайте новую"), "Dashboard")

        # Вкладка «Чекер» — полноценная проверка активности (Шаг 3)
        if self.client_manager is not None and self.activity_checker is not None:
            self.checker_tab = CheckerTab(
                self.db,
                self.client_manager,
                self.activity_checker,
                threshold_days_default=self.threshold_days_default,
            )
            # Трансляция событий чекера в панель логов главного окна
            self.checker_tab.check_started.connect(
                lambda n: self.log_message("INFO", f"Начата проверка {n} каналов")
            )
            self.checker_tab.check_completed.connect(
                lambda n: self.log_message("INFO", f"Проверка завершена, результатов: {n}")
            )
            self.checker_tab.check_error.connect(
                lambda err: self.log_message("ERROR", f"Ошибка проверки: {err}")
            )
            self.tabs.addTab(self.checker_tab, "Чекер")
        else:
            # Резервная заглушка, если модули ещё не инициализированы
            self.checker_tab = None
            self.tabs.addTab(self._create_placeholder_tab(
                "Checker", "Введите список каналов для проверки"), "Чекер")

        # Вкладка «Аккаунты» — полноценный экран управления (Шаг 2)
        if self.client_manager is not None:
            self.accounts_tab = AccountsWindow(self.db, self.client_manager)
            self.accounts_tab.accounts_changed.connect(self._on_accounts_changed)
        else:
            # Резервный вариант без менеджера клиентов (не должно случаться)
            self.accounts_tab = None
            self.tabs.addTab(self._create_placeholder_tab(
                "Accounts", "Управление аккаунтами Telegram"), "Аккаунты")
        if self.accounts_tab is not None:
            self.tabs.addTab(self.accounts_tab, "Аккаунты")

        self.tabs.addTab(self._create_placeholder_tab(
            "Settings", "Глобальные настройки приложения"), "Настройки")
        splitter.addWidget(self.tabs)

        # --- Панель логов ---
        log_panel = QWidget()
        log_layout = QVBoxLayout(log_panel)
        log_layout.setContentsMargins(0, 4, 0, 0)

        header = QHBoxLayout()
        header.addWidget(QLabel("Лог:"))
        # Фильтр по уровню
        self.level_filter = QComboBox()
        self.level_filter.addItems(["ALL", "INFO", "WARNING", "ERROR"])
        self.level_filter.setCurrentText("ALL")
        self.level_filter.currentTextChanged.connect(self._apply_log_filter)
        header.addWidget(self.level_filter)
        header.addStretch(1)
        log_layout.addLayout(header)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("Consolas", 9))
        log_layout.addWidget(self.log_view)

        splitter.addWidget(log_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)

        layout.addWidget(splitter)

        # Хранилище всех сообщений для перерисовки при смене фильтра
        self._log_entries: list[tuple[str, str]] = []
        self._current_filter: str = "ALL"

    def _create_placeholder_tab(self, name: str, text: str) -> QWidget:
        """
        Создание временной вкладки-заглушки с центрированной надписью.

        :param name: Имя вкладки (для objectName).
        :param text: Отображаемый текст.
        """
        widget = QWidget()
        widget.setObjectName(f"tab_{name}")
        vbox = QVBoxLayout(widget)
        label = QLabel(text)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("color: #777; font-size: 14px;")
        vbox.addWidget(label)
        return widget

    # ------------------------------------------------------------------
    # Панель логов
    # ------------------------------------------------------------------
    def log_message(self, level: str, message: str) -> None:
        """
        Добавление строки в панель логов с цветом по уровню.

        INFO — чёрный, WARNING — оранжевый, ERROR — красный.

        :param level: Уровень ('INFO' / 'WARNING' / 'ERROR').
        :param message: Текст сообщения.
        """
        level = level.upper()
        if level not in _LEVEL_COLORS:
            level = "INFO"

        self._log_entries.append((level, message))

        # Применяем текущий фильтр
        if self._current_filter != "ALL" and self._current_filter != level:
            return

        self._append_colored_line(level, message)

    def _append_colored_line(self, level: str, message: str) -> None:
        """Добавление раскрашенной строки в QTextEdit."""
        cursor = self.log_view.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)

        fmt = QTextCharFormat()
        fmt.setForeground(QColor(_LEVEL_COLORS.get(level, "#000000")))
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} | {level:<8} | {message}\n"

        cursor.insertText(line, fmt)
        self.log_view.setTextCursor(cursor)
        self.log_view.ensureCursorVisible()

    def _apply_log_filter(self, level: str) -> None:
        """Перерисовка панели логов с учётом выбранного фильтра уровней."""
        self._current_filter = level
        self.log_view.clear()
        for entry_level, message in self._log_entries:
            if level == "ALL" or entry_level == level:
                self._append_colored_line(entry_level, message)

    # ------------------------------------------------------------------
    # Индикатор статуса
    # ------------------------------------------------------------------
    def _on_accounts_changed(self, active_accounts: int) -> None:
        """Слот: вкладка «Аккаунты» сообщила об изменении числа активных аккаунтов."""
        self.update_status_indicator(self._active_tasks_count, active_accounts)

    def update_status_indicator(self, active_tasks: int, active_accounts: int) -> None:
        """
        Обновление текста индикатора на верхней панели.

        :param active_tasks: Число активных задач.
        :param active_accounts: Число активных аккаунтов.
        """
        self._active_tasks_count = active_tasks
        self._active_accounts_count = active_accounts
        self.status_indicator.setText(
            f"Активных задач: {active_tasks} | "
            f"Активных аккаунтов: {active_accounts}"
        )

    # ------------------------------------------------------------------
    # Обработчики событий окон
    # ------------------------------------------------------------------
    def closeEvent(self, event) -> None:  # noqa: N802 (Qt API)
        """Корректное закрытие окна: остановка фоновых воркеров."""
        self.log_message("INFO", "Завершение работы приложения")
        try:
            if getattr(self, "checker_tab", None) is not None:
                self.checker_tab.shutdown()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Ошибка остановки чекера: {exc}")
        super().closeEvent(event)
