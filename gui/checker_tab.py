# -*- coding: utf-8 -*-
"""
Вкладка «Чекер» — быстрая проверка активности каналов без создания задачи.

Возможности:
- ввод списка каналов (@username / t.me/... построчно) или загрузка из TXT/CSV;
- выбор порога активности (дней) и аккаунта(ов) для проверки;
- потоковое заполнение таблицы результатов с цветовой индикацией статуса;
- экспорт результатов (заглушка до следующего шага) и очистка таблицы.

Все сетевые операции Telethon выполняются в фоновом AsyncWorker (отдельный
поток со своим asyncio event loop), GUI не блокируется; результаты доставляются
в интерфейс через Qt-сигналы.
"""

import asyncio
import csv
import io
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from PySide6.QtCore import QObject, Qt, Signal, Slot
from PySide6.QtGui import QBrush, QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from database.db import DatabaseManager
from database.models import Account
from gui.accounts_window import AsyncWorker
from telegram_client.activity_checker import ActivityChecker
from telegram_client.client_manager import ClientManager

# Цвета статусов таблицы результатов
_COLOR_ACTIVE = QColor("#1e7d32")     # зелёный — активен
_COLOR_INACTIVE = QColor("#c62828")   # красный — неактивен
_COLOR_NO_ACCESS = QColor("#b8860b")  # жёлтый — нет доступа
_COLOR_ERROR = QColor("#757575")      # серый — ошибка

# Столбцы таблицы результатов
_COLUMNS = (
    "Username",
    "Название",
    "Подписчики",
    "Последний пост",
    "Последний коммент",
    "Доступ",
    "Статус",
)


class _BatchSignals(QObject):
    """
    Промежуточный объект-адаптер для потоковой доставки результатов.

    ActivityChecker работает в фоновом asyncio-потоке, а GUI-таблица должна
    обновляться только из Qt-потока. Сигналы этого объекта автоматически
    маршалируются в главный поток (Queued connection).
    """

    result_ready = Signal(dict)  # один результат проверки канала


class CheckerTab(QWidget):
    """Вкладка быстрой проверки активности каналов."""

    # Сигналы жизненного цикла пакетной проверки
    check_started = Signal(int)                      # количество каналов
    channel_checked = Signal(dict)                   # один результат
    check_completed = Signal(int)                    # всего результатов
    check_error = Signal(str)                        # текст ошибки

    ALL_ACCOUNTS_VALUE = -1                          # special item id для "Все аккаунты"

    def __init__(
        self,
        db: DatabaseManager,
        client_manager: ClientManager,
        activity_checker: ActivityChecker,
        threshold_days_default: int = 7,
        parent: Optional[QWidget] = None,
    ) -> None:
        """
        :param db: Менеджер базы данных (загрузка аккаунтов).
        :param client_manager: Менеджер Telethon-клиентов.
        :param activity_checker: Чекер активности (выполняет проверку).
        :param threshold_days_default: Порог активности по умолчанию (из настроек).
        :param parent: Родительский виджет.
        """
        super().__init__(parent)
        self.db = db
        self.client_manager = client_manager
        self.activity_checker = activity_checker

        self._running: bool = False           # идёт ли проверка сейчас
        self._total: int = 0                  # всего каналов в пакете
        self._done: int = 0                   # обработано каналов
        self._results: list[dict[str, Any]] = []

        # Фоновый поток asyncio для сетевых операций
        self.worker = AsyncWorker(self)
        self.worker.start()
        self.worker.task_finished.connect(self._on_worker_finished)
        self.worker.task_failed.connect(self._on_worker_failed)
        self.destroyed.connect(self._shutdown_worker)

        # Очередь обработчиков простых запросов к БД (FIFO)
        self._handlers: list[tuple[Any, Any]] = []
        # Выделенные колбэки пакетной проверки (вне FIFO)
        self._batch_done_callback: Optional[Any] = None
        self._batch_err_callback: Optional[Any] = None

        # Адаптер потоковой доставки результатов из asyncio-потока в Qt
        self._batch_signals = _BatchSignals()
        self._batch_signals.result_ready.connect(self.on_channel_checked)

        self._build_ui()

        # Порог активности по умолчанию из настроек (передан конструктором)
        self.spin_threshold.setValue(int(threshold_days_default))

        # Первичная загрузка списка аккаунтов
        self.load_accounts()

    def _shutdown_worker(self) -> None:
        """Остановка фонового потока при уничтожении виджета."""
        try:
            self.worker.stop()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Ошибка остановки воркера чекера: {exc}")

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        """Создание элементов вкладки."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        splitter = QSplitter(Qt.Orientation.Vertical)

        # --- Верхняя панель управления ---
        top_widget = QWidget()
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)

        # Список каналов
        self.channels_edit = QTextEdit()
        self.channels_edit.setPlaceholderText(
            "Каждый @username или t.me/username с новой строки"
        )
        self.channels_edit.setMaximumHeight(110)
        top_layout.addWidget(self.channels_edit)

        controls = QHBoxLayout()
        self.btn_load_file = QPushButton("Загрузить из файла")
        controls.addWidget(self.btn_load_file)

        controls.addWidget(QLabel("Порог активности (дни):"))
        self.spin_threshold = QSpinBox()
        self.spin_threshold.setRange(1, 3650)
        self.spin_threshold.setValue(7)
        controls.addWidget(self.spin_threshold)

        controls.addWidget(QLabel("Аккаунт для проверки:"))
        self.account_combo = QComboBox()
        self.account_combo.addItem("Все аккаунты", self.ALL_ACCOUNTS_VALUE)
        controls.addWidget(self.account_combo)

        self.btn_refresh_accounts = QPushButton("⟳")
        self.btn_refresh_accounts.setToolTip("Обновить список аккаунтов")
        controls.addWidget(self.btn_refresh_accounts)

        controls.addStretch(1)

        self.btn_start = QPushButton("Начать проверку")
        controls.addWidget(self.btn_start)
        top_layout.addLayout(controls)

        splitter.addWidget(top_widget)

        # --- Таблица результатов ---
        self.table = QTableWidget(0, len(_COLUMNS))
        self.table.setHorizontalHeaderLabels(list(_COLUMNS))
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        for col in range(2, len(_COLUMNS)):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)

        splitter.addWidget(self.table)

        # --- Нижняя панель ---
        bottom = QHBoxLayout()
        self.progress_label = QLabel("Прогресс: 0/0")
        bottom.addWidget(self.progress_label)
        bottom.addStretch(1)

        self.btn_export = QPushButton("Экспорт результатов")
        self.btn_clear = QPushButton("Очистить таблицу")
        bottom.addWidget(self.btn_export)
        bottom.addWidget(self.btn_clear)

        layout.addWidget(splitter, stretch=1)
        layout.addLayout(bottom)

        # Обработчики кнопок
        self.btn_load_file.clicked.connect(self.load_from_file)
        self.btn_start.clicked.connect(self.start_check)
        self.btn_clear.clicked.connect(self.clear_table)
        self.btn_export.clicked.connect(self.on_export_clicked)
        self.btn_refresh_accounts.clicked.connect(self.load_accounts)

    # ------------------------------------------------------------------
    # Загрузка аккаунтов в комбobox
    # ------------------------------------------------------------------
    def load_accounts(self) -> None:
        """Асинхронная загрузка активных аккаунтов из БД в список выбора."""
        current_data = self.account_combo.currentData()

        def on_accounts(accounts: list) -> None:
            """Заполнение комбoboxа активными аккаунтами."""
            self.account_combo.blockSignals(True)
            self.account_combo.clear()
            self.account_combo.addItem("Все аккаунты", self.ALL_ACCOUNTS_VALUE)
            for acc in accounts:
                if isinstance(acc, Account) and (acc.status or "active") == "active":
                    label = (
                        f"id={acc.id} @{acc.username or ''}".strip()
                        if acc.username
                        else f"id={acc.id} {str(acc.phone or '')[:3]}***"
                    )
                    self.account_combo.addItem(label, acc.id)
            # Восстановление выбора
            idx = self.account_combo.findData(current_data)
            self.account_combo.setCurrentIndex(max(idx, 0))
            self.account_combo.blockSignals(False)
            if self.account_combo.count() == 1:
                logger.warning("В БД нет активных аккаунтов — проверка невозможна")

        def on_error(err: str) -> None:
            logger.error(f"Ошибка загрузки аккаунтов в чекер: {err}")

        self._run_async(self.db.get_all_accounts, on_accounts, on_error)

    # ------------------------------------------------------------------
    # Механизм асинхронных вызовов (один воркер, очередь обработчиков)
    # ------------------------------------------------------------------
    # Примечание: простые запросы к БД используют FIFO-очередь _handlers,
    # а долгая пакетная проверка — выделенные колбэки self._batch_*_callback.
    def _run_async(
        self,
        coro_factory: Any,
        on_success: Any,
        on_error: Any,
    ) -> None:
        """Запрос в воркер с регистрацией колбэков в FIFO-очереди."""
        self._handlers.append((on_success, on_error))
        self.worker.submit(coro_factory)

    def _on_worker_finished(self, result: Any) -> None:
        """Доставка результата: сначала пакетный колбэк, иначе FIFO."""
        if self._batch_done_callback is not None:
            cb, self._batch_done_callback = self._batch_done_callback, None
            cb(result)
            return
        if self._handlers:
            on_ok, _ = self._handlers.pop(0)
            on_ok(result)

    def _on_worker_failed(self, error: str) -> None:
        """Доставка ошибки: сначала пакетный колбэк, иначе FIFO."""
        if self._batch_err_callback is not None:
            cb, self._batch_err_callback = self._batch_err_callback, None
            cb(error)
            return
        if self._handlers:
            _, on_err = self._handlers.pop(0)
            on_err(error)

    # ------------------------------------------------------------------
    # Загрузка списка каналов из файла
    # ------------------------------------------------------------------
    def load_from_file(self) -> None:
        """Открытие диалога выбора TXT/CSV файла и заполнение поля списка."""
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Загрузить список каналов",
            "",
            "Текстовые/CSV файлы (*.txt *.csv);;Все файлы (*)",
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as fh:
                raw = fh.read()
        except UnicodeDecodeError:
            # Резервная попытка в cp1251 (типично для Windows-файлов)
            try:
                with open(path, "r", encoding="cp1251", newline="") as fh:
                    raw = fh.read()
            except Exception as exc:  # noqa: BLE001
                QMessageBox.critical(self, "Ошибка чтения", f"Не удалось прочитать файл: {exc}")
                return
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "Ошибка чтения", f"Не удалось прочитать файл: {exc}")
            return

        lines: list[str] = []
        # CSV: берём первую колонку каждой строки; TXT: каждую непустую строку
        if path.lower().endswith(".csv"):
            try:
                reader = csv.reader(io.StringIO(raw))
                for row in reader:
                    if row and row[0].strip():
                        lines.append(row[0].strip())
            except csv.Error as exc:
                logger.warning(f"CSV разобран с ошибкой ({exc}),_used построчный режим")
                lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        else:
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

        self.channels_edit.setPlainText("\n".join(lines))
        logger.info(f"Загружено каналов из файла: {len(lines)}")

    # ------------------------------------------------------------------
    # Запуск проверки
    # ------------------------------------------------------------------
    def start_check(self) -> None:
        """Валидация входных данных и запуск пакетной проверки в воркере."""
        if self._running:
            QMessageBox.information(self, "Идёт проверка", "Дождитесь завершения текущей проверки.")
            return

        channels = [
            ln.strip() for ln in self.channels_edit.toPlainText().splitlines()
            if ln.strip()
        ]
        if not channels:
            QMessageBox.warning(
                self, "Пустой список",
                "Введите список каналов или загрузите его из файла.",
            )
            return

        selected_id = self.account_combo.currentData()
        account_ids: list[int] = []
        if selected_id == self.ALL_ACCOUNTS_VALUE:
            account_ids = [
                int(self.account_combo.itemData(i))
                for i in range(self.account_combo.count())
                if self.account_combo.itemData(i) != self.ALL_ACCOUNTS_VALUE
            ]
        elif selected_id is not None:
            account_ids = [int(selected_id)]

        if not account_ids:
            QMessageBox.warning(
                self, "Нет аккаунтов",
                "В БД нет активных аккаунтов. Добавьте аккаунт на вкладке «Аккаунты».",
            )
            return

        # Обновляем порог активности в настройках, чтобы чекер использовал его
        threshold = int(self.spin_threshold.value())

        self._running = True
        self._total = len(channels)
        self._done = 0
        self._results = []
        self.btn_start.setEnabled(False)
        self.progress_label.setText(f"Прогресс: 0/{self._total}")
        self.check_started.emit(self._total)
        logger.info(
            f"Запуск проверки {self._total} каналов аккаунтами {account_ids}, "
            f"порог {threshold} дн."
        )

        async def run_batch() -> list[dict[str, Any]]:
            """Корутина пакетной проверки (выполняется в AsyncWorker)."""

            async def on_result(res: dict[str, Any]) -> None:
                """Сохранение в БД + потоковая доставка результата в GUI."""
                await self.activity_checker.save_result(res)
                # Сигнал QObject автоматически маршалируется в Qt-поток
                self._batch_signals.result_ready.emit(dict(res))

            # Сначала применяем порог из спинбокса к глобальным настройкам
            await self.activity_checker.settings.set_activity_threshold_days(threshold)
            return await self.activity_checker.check_batch(channels, account_ids, on_result)

        def on_completed(results: Any) -> None:
            """Завершение проверки."""
            self._running = False
            self.btn_start.setEnabled(True)
            count = len(results) if isinstance(results, list) else self._done
            self.progress_label.setText(f"Прогресс: {self._total}/{self._total}")
            self.check_completed.emit(count)
            logger.info(f"Проверка завершена, результатов: {count}")

        def on_failed(err: str) -> None:
            """Ошибка всей пакетной проверки."""
            self._running = False
            self.btn_start.setEnabled(True)
            self.check_error.emit(err)
            logger.error(f"Ошибка пакетной проверки: {err}")
            QMessageBox.critical(self, "Ошибка проверки", err)

        # Выделенные колбэки для пакетной задачи (вне FIFO-очереди)
        self._batch_done_callback = on_completed
        self._batch_err_callback = on_failed
        self.worker.submit(run_batch)

    # ------------------------------------------------------------------
    # Слот обработки одного результата (Qt-поток)
    # ------------------------------------------------------------------
    @Slot(dict)
    def on_channel_checked(self, res: dict) -> None:
        """Добавление строки результата в таблицу с цветовой индикацией."""
        self._done += 1
        self._results.append(res)
        self.progress_label.setText(f"Прогресс: {self._done}/{self._total}")

        row = self.table.rowCount()
        self.table.insertRow(row)

        username = str(res.get("username") or "")
        title = str(res.get("title") or "—")
        participants = res.get("participants_count")
        last_post = self._format_dt(res.get("last_post_date"))
        last_comment = self._format_dt(res.get("last_comment_date"))
        access = str(res.get("access_type") or "—")
        error_message = res.get("error_message")

        # Определение статуса и цвета
        if error_message and not res.get("access_type"):
            status_text, color = "❌ Ошибка", _COLOR_ERROR
        elif res.get("access_type") == "private_no_access":
            status_text, color = "⚠ Нет доступа", _COLOR_NO_ACCESS
        elif res.get("is_active"):
            status_text, color = "✓ Активен", _COLOR_ACTIVE
        elif res.get("error_message"):
            status_text, color = "❌ Ошибка", _COLOR_ERROR
        else:
            status_text, color = "✗ Неактивен", _COLOR_INACTIVE

        values = [
            username,
            title,
            f"{participants:,}".replace(",", " ") if participants is not None else "—",
            last_post,
            last_comment,
            access,
            status_text,
        ]
        brush = QBrush(color)
        for col, value in enumerate(values):
            item = QTableWidgetItem(value)
            if col == len(values) - 1:  # колонка «Статус»
                item.setForeground(brush)
            elif error_message and col == 0:
                item.setToolTip(str(error_message))
            self.table.setItem(row, col, item)

        self.table.scrollToBottom()

    @staticmethod
    def _format_dt(iso_value: Optional[str]) -> str:
        """Форматирование ISO-даты в локальный вид 'ГГГГ-ММ-ДД ЧЧ:ММ'."""
        if not iso_value:
            return "—"
        try:
            dt = datetime.fromisoformat(iso_value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone().strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return str(iso_value)

    # ------------------------------------------------------------------
    # Экспорт (заглушка до Шага 4) и очистка
    # ------------------------------------------------------------------
    def on_export_clicked(self) -> None:
        """Кнопка экспорта результатов (реализация — в следующем шаге)."""
        QMessageBox.information(
            self, "Экспорт",
            "Экспорт результатов будет реализован в следующем шаге разработки.",
        )

    def clear_table(self) -> None:
        """Очистка таблицы результатов и счётчика прогресса."""
        self.table.setRowCount(0)
        self._results = []
        self._done = 0
        self._total = 0
        self.progress_label.setText("Прогресс: 0/0")

    # ------------------------------------------------------------------
    # Корректное завершение
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Остановка воркера при закрытии приложения."""
        self._shutdown_worker()
