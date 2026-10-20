# -*- coding: utf-8 -*-
"""
Модуль шифрования данных приложения.

Использует AES-256 (через Fernet) для симметричного шифрования и
PBKDF2-HMAC-SHA256 (100 000 итераций) для вывода ключа из мастер-пароля.
"""

import base64
import os
import secrets
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from loguru import logger

# Количество итераций PBKDF2 согласно ТЗ
PBKDF2_ITERATIONS: int = 100_000
# Размер соли в байтах
SALT_SIZE: int = 16


class EncryptionManager:
    """Менеджер шифрования/дешифрования чувствительных данных."""

    def __init__(self, master_password: str) -> None:
        """
        Инициализация менеджера шифрования.

        Генерирует новый случайный соль и выводит ключ через PBKDF2.

        :param master_password: Мастер-пароль пользователя.
        """
        if not master_password:
            raise ValueError("Мастер-пароль не может быть пустым")

        # Случайная соль для PBKDF2
        self.salt: bytes = secrets.token_bytes(SALT_SIZE)
        self._cipher_key: bytes = self._derive_key(master_password)
        self._fernet: Fernet = Fernet(self._cipher_key)
        logger.debug("EncryptionManager инициализирован (новый ключ)")

    def _derive_key(self, master_password: str, salt: Optional[bytes] = None) -> bytes:
        """
        Вывод 32-байтного ключа из пароля с помощью PBKDF2-HMAC-SHA256.

        :param master_password: Мастер-пароль.
        :param salt: Соль (если None — используется self.salt).
        :return: URL-safe base64-кодированный ключ для Fernet.
        """
        use_salt = salt if salt is not None else self.salt
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=use_salt,
            iterations=PBKDF2_ITERATIONS,
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_password.encode("utf-8")))
        return key

    def encrypt(self, data: str) -> str:
        """
        Шифрование строки (AES-256 через Fernet).

        :param data: Исходная строка.
        :return: Зашифрованная строка (base64 токен Fernet).
        """
        try:
            token = self._fernet.encrypt(data.encode("utf-8"))
            return token.decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка шифрования данных: {exc}")
            raise

    def decrypt(self, encrypted_data: str) -> str:
        """
        Расшифровка строки.

        :param encrypted_data: Зашифрованная строка (токен Fernet).
        :return: Исходная строка.
        :raises ValueError: Если токен некорректен или пароль не подходит.
        """
        try:
            decrypted = self._fernet.decrypt(encrypted_data.encode("utf-8"))
            return decrypted.decode("utf-8")
        except InvalidToken as exc:
            logger.error("Ошибка расшифровки: неверный токен или мастер-пароль")
            raise ValueError("Не удалось расшифровать данные: неверный ключ") from exc
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка расшифровки данных: {exc}")
            raise

    def save_master_key(self, key_path: str = "data/master.key") -> None:
        """
        Сохранение материала ключа (соли + зашифрованного проверочного токена)
        на диск. Файл содержит:
          - соль (16 байт);
          - Fernet-токен фиксированной строки-маркера, зашифрованный
            тем же ключом (позволяет проверить правильность пароля при загрузке).

        :param key_path: Путь к файлу ключа.
        """
        try:
            os.makedirs(os.path.dirname(key_path) or ".", exist_ok=True)
            marker_token = self._fernet.encrypt(b"TELEGRAM_DISCOVERY_MASTER_OK")
            with open(key_path, "wb") as f:
                f.write(self.salt)      # 16 байт соли
                f.write(marker_token)   # проверочный токен
            logger.info(f"Мастер-ключ сохранён в файл: {key_path}")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка сохранения мастер-ключа: {exc}")
            raise

    def load_master_key(self, key_path: str = "data/master.key",
                        master_password: Optional[str] = None) -> bool:
        """
        Загрузка ключа с диска и проверка мастер-пароля.

        Если передан ``master_password``, соль читается из файла и ключ
        заново выводится из пароля; успешная дешифровка маркера означает,
        что пароль верный.

        :param key_path: Путь к файлу ключа.
        :param master_password: Проверяемый мастер-пароль (опционально).
        :return: True, если ключ загружен и пароль подтверждён, иначе False.
        """
        try:
            if not os.path.exists(key_path):
                logger.warning(f"Файл мастер-ключа не найден: {key_path}")
                return False

            with open(key_path, "rb") as f:
                blob = f.read()

            if len(blob) <= SALT_SIZE:
                logger.error("Файл мастер-ключа повреждён")
                return False

            salt = blob[:SALT_SIZE]
            marker_token = blob[SALT_SIZE:]

            password = master_password if master_password is not None else ""
            key = self._derive_key(password, salt=salt)
            fernet = Fernet(key)
            marker = fernet.decrypt(marker_token)

            if marker == b"TELEGRAM_DISCOVERY_MASTER_OK":
                # Обновляем внутреннее состояние под загруженный ключ
                self.salt = salt
                self._cipher_key = key
                self._fernet = fernet
                logger.info("Мастер-ключ успешно загружен и проверен")
                return True

            logger.warning("Проверка мастер-ключа не пройдена (маркер не совпал)")
            return False
        except InvalidToken:
            logger.warning("Неверный мастер-пароль при загрузке ключа")
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Ошибка загрузки мастер-ключа: {exc}")
            return False
