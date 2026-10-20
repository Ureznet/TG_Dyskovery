# -*- coding: utf-8 -*-
"""
Экран управления аккаунтами Telegram (вкладка «Аккаунты»).

Содержит:
- таблицу аккаунтов (QTableWidget) с действиями «Редактировать» / «Удалить»;
- диалог добавления аккаунта (номер -> код -> 2FA) — AddAccountDialog;
- фоновый поток AsyncWorker для выполнения asyncio-операций Telethon/БД
  без блокировки GUI (результаты доставляются через Qt-сигналы).
"""

import asyncio
import html
import os
import re
from typing import Any, Callable, Coroutine, Optional

from loguru import logger
from PySide6.QtCore import QObject, QThread, Qt, Signal, Slot
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.db import DatabaseManager
from database.models import Account, Proxy
from telegram_client.client_manager import ClientManager


# ----------------------------------------------------------------------
# Фоновый воркер asyncio (один поток + собственный event loop)
# ----------------------------------------------------------------------
class AsyncWorker(QThread):
    """
    Поток с собственным asyncio event loop.

    Принимает фабрики корутин и выполняет их последовательно; результаты
    и ошибки доставляются в Qt-поток через сигналы (потокобезопасно).
    """

    task_finished = Signal(object)   # результат корутины
    task_failed = Signal(str)        # текст ошибки
    log_message = Signal(str, str)   # (level, message)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = True

    def run(self) -> None:
        """Тело потока: запуск собственного event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._process_queue())
        finally:
            self._loop.close()

    async def _process_queue(self) -> None:
        """Выборка и выполнение корутин из очереди до остановки потока."""
        while self._running:
            item = await self._queue.get()
            if item is None:  # сигнал остановки
                break
            coro_factory: Callable[[], Coroutine[Any, Any, Any]] = item
            try:
                result = await coro_factory()
                self.task_finished.emit(result)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Ошибка фоновой задачи: {exc}")
                self.task_failed.emit(str(exc))
            finally:
                self._queue.task_done()

    def submit(self, coro_factory: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        """Постановка фабрики корутины в очередь исполнения."""
        self._queue.put_nowait(coro_factory)

    def stop(self) -> None:
        """Остановка потока: укладываем цикл и завершаем воркер."""
        self._running = False
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait(None)
            )
        self.wait(5000)


# ----------------------------------------------------------------------
# Мост статусов из asyncio-потока в Qt-поток
# ----------------------------------------------------------------------
class _StatusBridge(QObject):
    """
    Вспомогательный QObject для потокобезопасной доставки этапов входа.

    Колбэки on_status вызываются из фоновых потоков (AsyncWorker /
    main.AsyncBridge); emit() Qt-сигнала из любого потока безопасен, а слот
    выполняется уже в потоке GUI (queued connection).
    """

    status = Signal(str)  # "connecting" | "connected" | "done" | "timeout"


# ----------------------------------------------------------------------
# Диалог добавления аккаунта
# ----------------------------------------------------------------------
_PHONE_RE = re.compile(r"^\+?[0-9]{5,16}$")


class AddAccountDialog(QDialog):
    """
    Диалог добавления аккаунта: номер телефона -> отправка кода ->
    ввод кода (+ опционально пароль 2FA) -> завершение входа.

    Все сетевые операции выполняются в переданном фоновом AsyncWorker,
    GUI при этом не блокируется.
    """

    account_added = Signal(object)  # созданный Account

    NO_PROXY_ID = -1

    def __init__(
        self, db: DatabaseManager, client_manager: ClientManager,
        worker: Optional[AsyncWorker] = None, parent: Optional[QWidget] = None,
        submitter: Optional[Callable[[Coroutine[Any, Any, Any]], None]] = None,
    ) -> None:
        """
        :param db: Менеджер БД (для загрузки списка прокси).
        :param client_manager: Менеджер Telethon-клиентов.
        :param worker: Фоновый поток исполнения asyncio-задач (AsyncWorker).
        :param parent: Родительский виджет.
        :param submitter: Альтернативный планировщик корутин (main.AsyncBridge).
                          Используется, если worker не передан; колбэки
                          результатов вызываются через QTimer.singleShot.
        """
        super().__init__(parent)
        self.db = db
        self.client_manager = client_manager
        self.worker = worker
        self._submitter = submitter

        if self.worker is None and self._submitter is None:
            raise ValueError(
                "AddAccountDialog: требуется либо worker, либо submitter"
            )

        # Данные шага 1 (отправка кода), необходимые для шага 2
        self._phone_code_hash: str = ""
        self._session_string: str = ""
        self._proxy_id: Optional[int] = None

        self.setWindowTitle("Добавление аккаунта Telegram")
        self.setModal(True)
        self.setMinimumWidth(460)

        # Очередь «разбирающих» колбэков: результат может относиться
        # к разным задачам, поэтому используется FIFO-очередь обработчиков.
        self._handlers: list[tuple[Callable[[Any], None], Callable[[str], None]]] = []

        # Мост статусов из asyncio-потока в Qt-поток
        self._status_bridge = _StatusBridge(self)
        self._status_bridge.status.connect(self._on_login_status)

        self._build_ui()
        self._connect_signals()

        if self.worker is not None:
            # Диспетчер результатов воркера (подключаем до постановки задач)
            self.worker.task_finished.connect(self._dispatch_result)
            self.worker.task_failed.connect(self._dispatch_failure)

        # Загрузка списка прокси сразу после открытия
        self._run_async(
            self.db.get_all_proxies,
            self._on_proxies_loaded,
            self._on_proxy_load_error,
        )

    # ------------------------- UI -------------------------
    def _build_ui(self) -> None:
        """Создание элементов диалога."""
        layout = QVBoxLayout(self)

        # --- Выбор способа добавления аккаунта ---
        mode_form = QFormLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItem("По номеру телефона (код SMS)", "phone")
        self.mode_combo.addItem("Импорт TData (AyuGram / Telegram Desktop)", "tdata")
        mode_form.addRow("Способ входа:", self.mode_combo)
        layout.addLayout(mode_form)

        # --- Панель A: вход по номеру телефона (код + 2FA) ---
        self.phone_panel = QWidget()
        phone_layout = QVBoxLayout(self.phone_panel)
        phone_layout.setContentsMargins(0, 0, 0, 0)

        form = QFormLayout()
        phone_layout.addLayout(form)

        self.phone_edit = QLineEdit()
        self.phone_edit.setPlaceholderText("+79991234567")
        self.phone_edit.setText("+7")
        form.addRow("Номер телефона:", self.phone_edit)

        # Кнопка отправки кода
        self.btn_send_code = QPushButton("Отправить код")
        phone_layout.addWidget(self.btn_send_code)

        # Шаг 2: код и облачный пароль (скрыт до успешной отправки кода)
        self.step2_widget = QWidget()
        form2 = QFormLayout(self.step2_widget)

        self.code_edit = QLineEdit()
        self.code_edit.setPlaceholderText("Код из SMS / звонка / приложения Telegram")
        form2.addRow("Код подтверждения:", self.code_edit)

        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_edit.setPlaceholderText("Облачный пароль (если включена 2FA)")
        form2.addRow("Пароль 2FA (опц.):", self.password_edit)

        # Подсказка про вход без SMS (облачный пароль)
        hint = QLabel(
            "SMS не приходит? Если на аккаунте включено двухфакторное "
            "шифрование — оставьте поле кода пустым, введите облачный "
            "пароль и нажмите «Войти по облачному паролю»."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #555; font-size: 11px;")
        form2.addRow(hint)

        self.step2_widget.setVisible(False)
        phone_layout.addWidget(self.step2_widget)

        self.btn_finish = QPushButton("Завершить вход")
        self.btn_finish.setEnabled(False)
        phone_layout.addWidget(self.btn_finish)

        # Отдельная кнопка входа сразу по облачному паролю (без SMS-кода)
        self.btn_password_login = QPushButton("Войти по облачному паролю (без SMS)")
        self.btn_password_login.setEnabled(False)
        self.btn_password_login.setToolTip(
            "Работает, если на аккаунте включена 2FA. Код из SMS при этом не нужен."
        )
        phone_layout.addWidget(self.btn_password_login)
        phone_layout.addStretch(1)

        layout.addWidget(self.phone_panel)

        # --- Панель B: импорт TData из десктопного клиента ---
        self.tdata_panel = QWidget()
        tdata_layout = QVBoxLayout(self.tdata_panel)
        tdata_layout.setContentsMargins(0, 0, 0, 0)

        tform = QFormLayout()
        tdata_layout.addLayout(tform)

        # Путь к папке tdata + кнопка выбора
        path_row = QHBoxLayout()
        self.tdata_path_edit = QLineEdit()
        self.tdata_path_edit.setPlaceholderText(
            "%APPDATA%\\AyuGramDesktop\\tdata\\Work"
        )
        self.btn_browse_tdata = QPushButton("Обзор…")
        path_row.addWidget(self.tdata_path_edit, 1)
        path_row.addWidget(self.btn_browse_tdata)
        tform.addRow("Папка tdata:", path_row)

        # Аккаунты, найденные в выбранной папке
        acc_row = QHBoxLayout()
        self.tdata_accounts_combo = QComboBox()
        self.tdata_accounts_combo.setToolTip(
            "Список аккаунтов, найденных в указанной папке tdata"
        )
        self.btn_scan_tdata = QPushButton("Найти аккаунты")
        acc_row.addWidget(self.tdata_accounts_combo, 1)
        acc_row.addWidget(self.btn_scan_tdata)
        tform.addRow("Аккаунт:", acc_row)

        # Пароль локального шифрования tdata (если задавался в клиенте)
        self.tdata_pass_edit = QLineEdit()
        self.tdata_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.tdata_pass_edit.setPlaceholderText(
            "Только если включено локальное шифрование Two-Factor в клиенте"
        )
        tform.addRow("Пароль tdata (опц.):", self.tdata_pass_edit)

        tdata_hint = QLabel(
            "Как получить путь: AyuGram → Настройки → Продвинутые → «Открыть "
            "папку с файлами» (или вручную: C:\\Users\\ВашНик\\AppData\\Roaming\\"
            "AyuGramDesktop\\tdata). Можно указать как корневую папку tdata "
            "(приложение сама найдёт все аккаунты), так и подпапку конкретного "
            "аккаунта. Перед импортом рекомендуется полностью закрыть AyuGram.\n\n"
            "Требуется пакет opentele: pip install opentele"
        )
        tdata_hint.setWordWrap(True)
        tdata_hint.setStyleSheet("color: #555; font-size: 11px;")
        tdata_layout.addWidget(tdata_hint)

        self.btn_import_tdata = QPushButton("Импортировать аккаунт из TData")
        tdata_layout.addWidget(self.btn_import_tdata)
        tdata_layout.addStretch(1)

        layout.addWidget(self.tdata_panel)

        # Общий выбор прокси для обоих способов
        proxy_form = QFormLayout()
        self.proxy_combo = QComboBox()
        self.proxy_combo.addItem("Без прокси", self.NO_PROXY_ID)
        proxy_form.addRow("Прокси:", self.proxy_combo)
        layout.addLayout(proxy_form)

        # Статусная строка
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #555;")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        btn_box = QHBoxLayout()
        btn_box.addStretch(1)
        self.btn_cancel = QPushButton("Отмена")
        btn_box.addWidget(self.btn_cancel)
        layout.addLayout(btn_box)

        # Изначально показан способ по номеру
        self._on_mode_changed(self.mode_combo.currentData())

    def _connect_signals(self) -> None:
        """Подключение кнопок."""
        self.btn_send_code.clicked.connect(self.on_send_code)
        self.btn_finish.clicked.connect(self.on_finish_login)
        self.btn_password_login.clicked.connect(self.on_password_login)
        self.btn_cancel.clicked.connect(self.reject)
        self.mode_combo.currentIndexChanged.connect(
            lambda _i: self._on_mode_changed(self.mode_combo.currentData())
        )
        self.btn_browse_tdata.clicked.connect(self.on_browse_tdata)
        self.btn_scan_tdata.clicked.connect(self.on_scan_tdata)
        self.btn_import_tdata.clicked.connect(self.on_import_tdata)

    # ------------------------- Режимы диалога -------------------------
    def _on_mode_changed(self, mode: str) -> None:
        """Переключение панелей в зависимости от выбранного способа входа."""
        self.phone_panel.setVisible(mode == "phone")
        self.tdata_panel.setVisible(mode == "tdata")
        if mode == "tdata":
            self.status_label.setText(
                "Укажите папку tdata (или нажмите «Найти аккаунты» после "
                "выбора пути)."
            )
        else:
            self.status_label.setText("")

    @Slot(str)
    def _on_login_status(self, stage: str) -> None:
        """Отображение текущего этапа сетевого взаимодействия с Telegram."""
        texts = {
            "connecting": "Подключение к серверам Telegram…",
            "connected": "Соединение установлено. Отправка запроса…",
            "done": "Готово.",
            "timeout": "Серверы Telegram не отвечают (таймаут соединения).",
            "tdata_convert": "Чтение и конвертация TData…",
            "tdata_converted": "TData сконвертирован. Проверка сессии…",
        }
        self.status_label.setText(texts.get(stage, stage))

    # ------------------------- Результаты -------------------------
    @Slot(object)
    def _dispatch_result(self, result: Any) -> None:
        """Передача результата текущему обработчику (FIFO)."""
        if self._handlers:
            on_ok, _ = self._handlers.pop(0)
            on_ok(result)

    @Slot(str)
    def _dispatch_failure(self, error: str) -> None:
        """Передача ошибки текущему обработчику (FIFO)."""
        if self._handlers:
            _, on_err = self._handlers.pop(0)
            on_err(error)

    def _run_async(
        self,
        coro_factory: Callable[[], Coroutine[Any, Any, Any]],
        on_success: Callable[[Any], None],
        on_error: Callable[[str], None],
    ) -> None:
        """
        Запуск asyncio-задачи с регистрацией колбэков (FIFO).

        Поддерживает два исполнителя: AsyncWorker (QThread) или внешний
        submitter (main.AsyncBridge) — колбэки всегда доставляются в Qt-поток.
        """
        self._handlers.append((on_success, on_error))
        if self.worker is not None:
            self.worker.submit(coro_factory)
            return

        # Режим внешнего планировщика (AsyncBridge): подписываемся на
        # Future и переносим результат в Qt-поток через QTimer.singleShot.
        from PySide6.QtCore import QTimer

        try:
            future = self._submitter(coro_factory())  # type: ignore[misc]
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка планирования задачи диалога: {exc}")
            # Снимаем обработчик, чтобы не смещалась FIFO-очередь
            self._handlers.pop()
            QTimer.singleShot(0, lambda t=str(exc): self._dispatch_failure(t))
            return

        def _done(fut: Any) -> None:
            """Колбэк завершения в asyncio-потоке — переносим в Qt-поток."""
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                text = str(exc) or exc.__class__.__name__
                logger.error(f"Ошибка фоновой задачи диалога: {text}")
                QTimer.singleShot(0, lambda t=text: self._dispatch_failure(t))
                return
            QTimer.singleShot(0, lambda r=result: self._dispatch_result(r))

        try:
            future.add_done_callback(_done)
        except Exception as exc:  # noqa: BLE001
            # Future уже завершён/некорректен — обрабатываем сразу
            logger.error(f"Не удалось зарегистрировать колбэк задачи: {exc}")
            try:
                result = future.result()
            except Exception as task_exc:  # noqa: BLE001
                text = str(task_exc) or task_exc.__class__.__name__
                QTimer.singleShot(0, lambda t=text: self._dispatch_failure(t))
            else:
                QTimer.singleShot(0, lambda r=result: self._dispatch_result(r))

    @Slot(object)
    def _on_proxies_loaded(self, proxies: list) -> None:
        """Заполнение комбoboxа прокси списком из БД."""
        current = self.proxy_combo.currentData()
        for proxy in proxies:
            if isinstance(proxy, Proxy):
                self.proxy_combo.addItem(
                    f"{proxy.name} ({proxy.type} {proxy.host}:{proxy.port})", proxy.id
                )
        # Восстанавливаем прежний выбор, если он ещё существует
        idx = self.proxy_combo.findData(current)
        if idx >= 0:
            self.proxy_combo.setCurrentIndex(idx)

    def _on_proxy_load_error(self, error: str) -> None:
        """Ошибка загрузки списка прокси (не критично для диалога)."""
        logger.warning(f"Не удалось загрузить список прокси в диалог: {error}")

    # ------------------------- Шаг 1: отправка кода -------------------------
    def _validate_phone(self) -> Optional[str]:
        """Проверка и нормализация введённого номера."""
        raw = self.phone_edit.text().strip()
        normalized = re.sub(r"[\s\-\(\)]+", "", raw)
        if not _PHONE_RE.match(normalized):
            QMessageBox.warning(
                self, "Неверный номер",
                "Введите номер в международном формате, например +79991234567.",
            )
            return None
        return normalized

    def on_send_code(self) -> None:
        """Отправка кода подтверждения на указанный номер."""
        phone = self._validate_phone()
        if phone is None:
            return

        proxy_data = self.proxy_combo.currentData()
        self._proxy_id = None if proxy_data == self.NO_PROXY_ID else int(proxy_data)

        self.btn_send_code.setEnabled(False)
        self.status_label.setText("Отправка кода...")

        def on_success(result: dict) -> None:
            """Код отправлен — открываем шаг 2."""
            self._phone_code_hash = result.get("phone_code_hash", "")
            self._session_string = result.get("session_string", "")
            self.step2_widget.setVisible(True)
            self.btn_finish.setEnabled(True)
            self.btn_password_login.setEnabled(True)
            self.btn_send_code.setEnabled(True)
            self.btn_send_code.setText("Отправить код ещё раз")
            self.status_label.setText(
                "Запрос кода выполнен. Проверьте SMS, звонок и приложение "
                "Telegram (код может прийти в существующую сессию). "
                "Если включена 2FA и SMS не приходит — используйте вход по "
                "облачному паролю."
            )
            self.code_edit.setFocus()

        def on_error(err: str) -> None:
            self.btn_send_code.setEnabled(True)
            self.status_label.setText("")
            QMessageBox.critical(self, "Ошибка отправки кода", err)

        # Колбэк статусов из asyncio-потока -> Qt-поток через мост
        status_cb = lambda stage: self._status_bridge.status.emit(stage)  # noqa: E731

        self._run_async(
            lambda: self.client_manager.add_account_by_phone(
                phone, self._proxy_id, on_status=status_cb
            ),
            on_success,
            on_error,
        )

    # ------------------------- Шаг 2: завершение входа -------------------------
    def _finish_login_common(
        self, code: str, password_2fa: Optional[str], login_mode: str
    ) -> None:
        """
        Общий код завершения входа (по коду или сразу по облачному паролю).

        :param code: Введённый код (пусто = вход только по паролю).
        :param password_2fa: Облачный пароль либо None.
        :param login_mode: Режим для статуса ("code" | "password").
        """
        phone = self._validate_phone()
        if phone is None:
            return

        if not self._session_string or not self._phone_code_hash:
            QMessageBox.warning(
                self, "Нет данных шага 1",
                "Сначала нажмите «Отправить код» — вход выполняется "
                "на той же сессии, что и запрос кода.",
            )
            return

        self.btn_finish.setEnabled(False)
        self.btn_password_login.setEnabled(False)
        self.btn_send_code.setEnabled(False)
        self.status_label.setText(
            "Вход по облачному паролю..." if login_mode == "password"
            else "Проверка кода..."
        )

        def on_success(account: Account) -> None:
            """Аккаунт создан — закрываем диалог с успехом."""
            self.account_added.emit(account)
            QMessageBox.information(
                self, "Успех",
                f"Аккаунт добавлен:\nID: {account.id}\n"
                f"Username: @{account.username or '—'}",
            )
            self.accept()

        def on_error(err: str) -> None:
            self.btn_finish.setEnabled(True)
            self.btn_password_login.setEnabled(True)
            self.btn_send_code.setEnabled(True)
            self.status_label.setText("")
            QMessageBox.critical(self, "Ошибка входа", err)

        # Колбэк статусов из asyncio-потока -> Qt-поток через мост
        status_cb = lambda stage: self._status_bridge.status.emit(stage)  # noqa: E731

        self._run_async(
            lambda: self.client_manager.complete_login(
                phone=phone,
                code=code,
                session_string=self._session_string,
                phone_code_hash=self._phone_code_hash,
                password_2fa=password_2fa,
                proxy_id=self._proxy_id,
                on_status=status_cb,
            ),
            on_success,
            on_error,
        )

    def on_finish_login(self) -> None:
        """Ввод кода (и 2FA при необходимости) и сохранение аккаунта в БД."""
        code = self.code_edit.text().strip()
        password_2fa = self.password_edit.text().strip() or None

        if not code and not password_2fa:
            QMessageBox.warning(
                self, "Нет данных для входа",
                "Введите код подтверждения либо облачный пароль (2FA).",
            )
            return

        self._finish_login_common(code, password_2fa, "code")

    def on_password_login(self) -> None:
        """
        Вход СРАЗУ по облачному паролю без SMS-кода.

        Подходит, когда Telegram не доставляет SMS (лимиты, блокировка
        доставки), но на аккаунте включена двухфакторная аутентификация:
        сервер принимает sign_in(password=...) без предварительного кода.
        """
        password_2fa = self.password_edit.text().strip() or None
        if not password_2fa:
            QMessageBox.warning(
                self, "Нет пароля",
                "Введите облачный пароль (2FA) перед входом без SMS-кода.",
            )
            return

        reply = QMessageBox.question(
            self, "Вход по облачному паролю",
            "Выполнить вход без ввода SMS-кода, только по облачному "
            "паролю (2FA)?\n\nЭтот способ работает, если на аккаунте "
            "включено двухфакторное шифрование.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._finish_login_common("", password_2fa, "password")

    # ------------------------- Импорт TData (AyuGram / Telegram Desktop) -------------------------
    @staticmethod
    def _default_tdata_paths() -> list[str]:
        """
        Типичные пути к папкам tdata на Windows.

        Проверяются AyuGram, iGram и официальный Telegram Desktop —
        все они используют структуру <AppData>/<клиент>/tdata/<аккаунт>.

        :return: Список существующих на машине путей (может быть пустым).
        """
        appdata = os.environ.get("APPDATA", "")
        candidates = []
        for client_dir in ("AyuGramDesktop", "iGramDesktop", "Telegram Desktop"):
            if appdata:
                candidates.append(os.path.join(appdata, client_dir, "tdata"))
        return [p for p in candidates if os.path.isdir(p)]

    def prefill_tdata_path(self) -> None:
        """Автозаполнение поля пути, если tdata найден автоматически."""
        for path in self._default_tdata_paths():
            self.tdata_path_edit.setText(path)
            self.on_scan_tdata()
            return

    def on_browse_tdata(self) -> None:
        """Выбор папки tdata через системный диалог."""
        start_dir = self.tdata_path_edit.text().strip() or ""
        folder = QFileDialog.getExistingDirectory(
            self, "Выберите папку tdata (или папку клиента)", start_dir
        )
        if folder:
            self.tdata_path_edit.setText(folder)
            self.on_scan_tdata()

    def on_scan_tdata(self) -> None:
        """Сканирование указанной папки и заполнение списка аккаунтов."""
        base = self.tdata_path_edit.text().strip()
        self.tdata_accounts_combo.clear()
        if not base:
            self.status_label.setText("Сначала укажите папку tdata.")
            return

        accounts = ClientManager.find_tdata_accounts(base)
        if not accounts:
            self.status_label.setText(
                f"В папке «{base}» аккаунты tdata не найдены.\n"
                "Укажите путь к папке клиента (например "
                "%APPDATA%\\AyuGramDesktop) или к подпапке аккаунта внутри tdata."
            )
            return

        for name, path in accounts:
            # data — ПУТЬ К КОНТЕЙНЕРУ tdata целиком (не к подпапке):
            # opentele читает key_datas из корня; выбор конкретного
            # аккаунта выполняется по имени подпапки ("Аккаунт 3" -> 3).
            self.tdata_accounts_combo.addItem(name, path)
        self.status_label.setText(
            f"Найдено контейнеров tdata: {len(accounts)}. "
            "Выберите аккаунт и нажмите «Импортировать»."
        )

    def on_import_tdata(self) -> None:
        """Конвертация выбранного TData-аккаунта и сохранение в БД."""
        tdata_path = self.tdata_accounts_combo.currentData()
        if not tdata_path:
            # Возможно пользователь ввёл путь вручную — используем его
            manual = self.tdata_path_edit.text().strip()
            if manual and os.path.isdir(manual):
                tdata_path = manual
            else:
                QMessageBox.warning(
                    self, "Нет аккаунта",
                    "Укажите папку tdata и нажмите «Найти аккаунты», "
                    "затем выберите аккаунт из списка.",
                )
                return

        password = self.tdata_pass_edit.text().strip() or None

        # Имя выбранного аккаунта -> индекс подпапки для импорта из общего
        # контейнера key_datas ("Аккаунт 3" -> "3", "Work" -> "Work")
        acc_label = str(self.tdata_accounts_combo.currentText() or "").strip()
        account_name: Optional[str] = None
        if acc_label.startswith("Аккаунт "):
            account_name = acc_label.replace("Аккаунт ", "").strip() or None
        elif acc_label and not acc_label.startswith("Telegram Desktop"):
            account_name = acc_label

        proxy_data = self.proxy_combo.currentData()
        proxy_id = None if proxy_data == self.NO_PROXY_ID else int(proxy_data)

        self.btn_import_tdata.setEnabled(False)
        self.btn_send_code.setEnabled(False)
        self.status_label.setText("Импорт TData: идёт конвертация…")

        def on_success(account: Account) -> None:
            """Аккаунт импортирован — закрываем диалог с успехом."""
            self.account_added.emit(account)
            warning = getattr(account, "_verify_warning", None)
            msg = (
                f"Аккаунт импортирован из TData:\nID: {account.id}\n"
                f"Username: @{account.username or '—'}\n"
                f"Телефон: {account.phone or '—'}"
            )
            if warning:
                QMessageBox.information(
                    self, "Импорт завершён (с предупреждением)",
                    msg + f"\n\n⚠ {warning}",
                )
            else:
                QMessageBox.information(self, "Успех", msg)
            self.accept()

        def on_error(err: str) -> None:
            self.btn_import_tdata.setEnabled(True)
            self.btn_send_code.setEnabled(True)
            self.status_label.setText("Ошибка импорта. См. окно ошибки.")
            QMessageBox.critical(self, "Ошибка импорта TData", err)

        status_cb = lambda stage: self._status_bridge.status.emit(stage)  # noqa: E731

        self._run_async(
            lambda: self.client_manager.add_account_by_tdata(
                str(tdata_path), password=password, proxy_id=proxy_id,
                on_status=status_cb, account_name=account_name,
            ),
            on_success,
            on_error,
        )


# ----------------------------------------------------------------------
# Экран управления аккаунтами
# ----------------------------------------------------------------------
_STATUS_ITEMS = ("active", "flood_waited", "banned", "invalid", "sleep", "error")


class AccountsWindow(QWidget):
    """
    Вкладка «Аккаунты»: таблица аккаунтов + кнопки действий.

    Все обращения к БД выполняются через фоновый AsyncWorker,
    чтобы не блокировать интерфейс.
    """

    accounts_changed = Signal(int)  # количество активных аккаунтов

    def __init__(
        self, db: DatabaseManager, client_manager: ClientManager,
        parent: Optional[QWidget] = None,
        submitter: Optional[Callable[[Coroutine[Any, Any, Any]], None]] = None,
    ) -> None:
        """
        :param db: Менеджер базы данных.
        :param client_manager: Менеджер Telethon-клиентов.
        :param parent: Родительский виджет.
        :param submitter: Планировщик корутин в общем asyncio-потоке приложения
                          (main.AsyncBridge.submit). Если не передан — создаётся
                          собственный AsyncWorker (QThread).
        """
        super().__init__(parent)
        self.db = db
        self.client_manager = client_manager
        self._submitter = submitter

        # Фоновый исполнитель asyncio-задач для этого экрана:
        # общий мост приложения либо собственный поток
        self.worker: Optional[AsyncWorker] = None
        if self._submitter is None:
            self.worker = AsyncWorker(self)
            self.worker.start()
            # Связываем завершение потока с очисткой
            self.destroyed.connect(self._shutdown_worker)

        self._accounts: list[Account] = []
        self._handlers: list[tuple[Callable[[Any], None], Callable[[str], None]]] = []

        self._build_ui()
        self.load_accounts()

    def _shutdown_worker(self) -> None:
        """Остановка фонового потока при уничтожении виджета."""
        try:
            if self.worker is not None:
                self.worker.stop()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Ошибка остановки воркера аккаунтов: {exc}")

    # ------------------------- UI -------------------------
    def _build_ui(self) -> None:
        """Создание панели инструментов и таблицы."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Верхняя панель
        top = QHBoxLayout()
        self.btn_add = QPushButton("Добавить аккаунт")
        self.btn_refresh = QPushButton("Обновить список")
        top.addWidget(self.btn_add)
        top.addWidget(self.btn_refresh)
        top.addStretch(1)
        self.info_label = QLabel("Аккаунтов: 0")
        top.addWidget(self.info_label)
        layout.addLayout(top)

        self.btn_add.clicked.connect(self.open_add_dialog)
        self.btn_refresh.clicked.connect(self.load_accounts)

        # Таблица аккаунтов
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["ID", "Телефон / Username", "Статус", "Прокси", "Последний запрос", "Действия"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)

        layout.addWidget(self.table)

        # Подключаем диспетчер результатов воркера (если используется свой поток)
        if self.worker is not None:
            self.worker.task_finished.connect(self._dispatch_result)
            self.worker.task_failed.connect(self._dispatch_failure)

    # ------------------------- Диспетчер задач -------------------------
    def _run_async(
        self,
        coro_factory: Callable[[], Coroutine[Any, Any, Any]],
        on_success: Callable[[Any], None],
        on_error: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Запуск задачи в воркере с очередью обработчиков (FIFO)."""
        self._handlers.append((on_success, on_error or self._default_error))
        if self.worker is not None:
            self.worker.submit(coro_factory)
            return

        # Режим общего моста приложения (main.AsyncBridge)
        from PySide6.QtCore import QTimer

        try:
            future = self._submitter(coro_factory())  # type: ignore[misc]
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка планирования задачи аккаунтов: {exc}")
            self._handlers.pop()  # не смещаем FIFO-очередь обработчиков
            QTimer.singleShot(0, lambda t=str(exc): self._dispatch_failure(t))
            return

        def _done(fut: Any) -> None:
            """Колбэк завершения в asyncio-потоке — переносим в Qt-поток."""
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                text = str(exc) or exc.__class__.__name__
                logger.error(f"Ошибка фоновой задачи аккаунтов: {text}")
                QTimer.singleShot(0, lambda t=text: self._dispatch_failure(t))
                return
            QTimer.singleShot(0, lambda r=result: self._dispatch_result(r))

        try:
            future.add_done_callback(_done)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Не удалось зарегистрировать колбэк задачи: {exc}")
            try:
                result = future.result()
            except Exception as task_exc:  # noqa: BLE001
                text = str(task_exc) or task_exc.__class__.__name__
                QTimer.singleShot(0, lambda t=text: self._dispatch_failure(t))
            else:
                QTimer.singleShot(0, lambda r=result: self._dispatch_result(r))

    @Slot(object)
    def _dispatch_result(self, result: Any) -> None:
        if self._handlers:
            on_ok, _ = self._handlers.pop(0)
            on_ok(result)

    @Slot(str)
    def _dispatch_failure(self, error: str) -> None:
        if self._handlers:
            _, on_err = self._handlers.pop(0)
            on_err(error)

    def _default_error(self, error: str) -> None:
        """Обработчик ошибок по умолчанию."""
        logger.error(f"Ошибка операции с аккаунтами: {error}")
        QMessageBox.critical(self, "Ошибка", error)

    # ------------------------- Загрузка данных -------------------------
    def load_accounts(self) -> None:
        """Загрузка списка аккаунтов и прокси из БД и заполнение таблицы."""
        def on_accounts(accounts: list) -> None:
            """Кэш аккаунтов получен — запрашиваем прокси для имён."""
            self._accounts = list(accounts)
            self._run_async(self.db.get_all_proxies, lambda proxies: self._fill_table(proxies))

        self._run_async(self.db.get_all_accounts, on_accounts)

    def _fill_table(self, proxies: list) -> None:
        """Заполнение QTableWidget данными аккаунтов."""
        proxy_names: dict[int, str] = {
            p.id: p.name for p in proxies if isinstance(p, Proxy)
        }

        self.table.setRowCount(0)
        active_count = 0
        for acc in self._accounts:
            if not isinstance(acc, Account):
                continue
            row = self.table.rowCount()
            self.table.insertRow(row)

            # ID
            self.table.setItem(row, 0, QTableWidgetItem(str(acc.id)))

            # Телефон / Username
            display_name = f"{acc.phone or '—'}"
            if acc.username:
                display_name += f"  (@{acc.username})"
            name_item = QTableWidgetItem(display_name)
            self.table.setItem(row, 1, name_item)

            # Статус
            status_item = QTableWidgetItem(acc.status or "unknown")
            if acc.status == "active":
                status_item.setForeground(Qt.GlobalColor.darkGreen)
                active_count += 1
            elif acc.status in ("banned", "invalid", "error"):
                status_item.setForeground(Qt.GlobalColor.red)
            elif acc.status in ("flood_waited", "sleep", "flood"):
                status_item.setForeground(Qt.GlobalColor.darkYellow)
            self.table.setItem(row, 2, status_item)

            # Прокси
            proxy_text = proxy_names.get(acc.proxy_id, "Нет") if acc.proxy_id else "Нет"
            self.table.setItem(row, 3, QTableWidgetItem(proxy_text))

            # Последний запрос
            self.table.setItem(row, 4, QTableWidgetItem(acc.last_used or "—"))

            # Действия
            self.table.setCellWidget(row, 5, self._make_actions_widget(acc))

        self.info_label.setText(
            f"Аккаунтов: {len(self._accounts)} | Активных: {active_count}"
        )
        self.accounts_changed.emit(active_count)

    def _make_actions_widget(self, account: Account) -> QWidget:
        """Создание виджета с кнопками «Редактировать» / «Удалить»."""
        widget = QWidget()
        hbox = QHBoxLayout(widget)
        hbox.setContentsMargins(2, 0, 2, 0)
        hbox.setSpacing(4)

        btn_edit = QPushButton("Редактировать")
        btn_edit.setFixedWidth(100)
        btn_edit.clicked.connect(lambda checked=False, a=account: self.edit_account(a))
        hbox.addWidget(btn_edit)

        btn_delete = QPushButton("Удалить")
        btn_delete.setFixedWidth(80)
        btn_delete.clicked.connect(lambda checked=False, a=account: self.delete_account(a))
        hbox.addWidget(btn_delete)

        return widget

    # ------------------------- Диалог добавления -------------------------
    def open_add_dialog(self) -> None:
        """Открытие диалога добавления аккаунта."""
        dialog = AddAccountDialog(
            self.db,
            self.client_manager,
            worker=self.worker,
            parent=self,
            submitter=self._submitter,
        )
        dialog.account_added.connect(self._on_account_added)
        dialog.exec()

    @Slot(object)
    def _on_account_added(self, account: Account) -> None:
        """Успешное добавление аккаунта — обновляем таблицу."""
        logger.info(f"Добавлен аккаунт id={account.id}")
        self.load_accounts()

    # ------------------------- Действия над аккаунтом -------------------------
    def edit_account(self, account: Account) -> None:
        """
        Редактирование аккаунта: username и статус.

        (Смена сессии/телефона не предусмотрена ТЗ Шага 2.)
        """
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Редактирование аккаунта id={account.id}")
        dialog.setModal(True)

        form = QFormLayout(dialog)

        username_edit = QLineEdit(account.username or "")
        username_edit.setPlaceholderText("username без @")
        form.addRow("Username:", username_edit)

        status_combo = QComboBox()
        status_combo.addItems(_STATUS_ITEMS)
        current = (account.status or "active").lower()
        idx = status_combo.findText(current)
        status_combo.setCurrentIndex(idx if idx >= 0 else 0)
        form.addRow("Статус:", status_combo)

        btn_box = QHBoxLayout()
        btn_save = QPushButton("Сохранить")
        btn_cancel = QPushButton("Отмена")
        btn_box.addStretch(1)
        btn_box.addWidget(btn_save)
        btn_box.addWidget(btn_cancel)
        form.addRow(btn_box)

        def save() -> None:
            """Применение изменений к объекту и запись в БД."""
            account.username = username_edit.text().strip().lstrip("@") or None
            account.status = status_combo.currentText()

            def on_saved(ok: bool) -> None:
                if ok:
                    logger.info(f"Аккаунт id={account.id} обновлён")
                    dialog.accept()
                    self.load_accounts()
                else:
                    QMessageBox.warning(dialog, "Ошибка", "Не удалось обновить аккаунт.")

            self._run_async(lambda: self.db.update_account(account), on_saved)

        btn_save.clicked.connect(save)
        btn_cancel.clicked.connect(dialog.reject)
        dialog.exec()

    def delete_account(self, account: Account) -> None:
        """Удаление аккаунта с подтверждением."""
        title = html.escape(f"аккаунт id={account.id} ({account.phone or account.username or '—'})")
        reply = QMessageBox.question(
            self, "Подтверждение удаления",
            f"Вы действительно хотите удалить {title}?\n"
            "Сохранённая сессия будет утрачена.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        def on_deleted(ok: bool) -> None:
            if ok:
                logger.info(f"Аккаунт id={account.id} удалён")
                self.load_accounts()
            else:
                QMessageBox.warning(self, "Ошибка", "Не удалось удалить аккаунт.")

        self._run_async(lambda: self.db.delete_account(int(account.id)), on_deleted)
