#!/usr/bin/env python3
"""
Конфигурация тестов для Yandex S3 модуля.
Упрощенная версия для GitHub Actions.
"""

import os


# Убираем вызов load_dotenv, так как в CI секреты приходят через переменные окружения
# from dotenv import load_dotenv

class TestConfig:
    """Конфигурация для тестов."""

    # Режим тестирования
    # В CI всегда используем 'real', если есть ключи
    TEST_MODE = 'real' if os.getenv('AWS_ACCESS_KEY_ID') else 'mock'

    # Учетные данные: сначала пробуем взять из переменных GitHub Actions,
    # затем из стандартных переменных Yandex Cloud
    YANDEX_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('YANDEX_ACCESS_KEY', '')
    YANDEX_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('YANDEX_SECRET_KEY', '')

    # Бакет можно передать через переменную окружения
    YANDEX_BUCKET = os.getenv('YANDEX_BUCKET', 'your-test-bucket-name')

    # Настройки тестовых данных
    TEST_FILE_CONTENT = b"Test content for Yandex Cloud S3 module testing"
    TEST_FILE_NAME = "test_file.txt"

    @classmethod
    def has_real_credentials(cls):
        """Проверяет наличие реальных учетных данных."""
        # Проверяем оба варианта
        has_via_aws = bool(cls.YANDEX_ACCESS_KEY and cls.YANDEX_SECRET_KEY)
        return has_via_aws

    @classmethod
    def skip_if_no_credentials(cls):
        """Декоратор для пропуска тестов без учетных данных."""
        import unittest
        def decorator(func):
            if not cls.has_real_credentials():
                return unittest.skip("Требуются реальные учетные данные Yandex Cloud")(func)
            return func

        return decorator