#!/usr/bin/env python3
"""
Конфигурация тестов для Yandex S3 модуля.
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

class TestConfig:
    """Конфигурация для тестов."""
    
    # Режим тестирования
    TEST_MODE = os.getenv('TEST_MODE', 'mock')  # 'mock' или 'real'
    
    # Учетные данные для реальных тестов (опционально)
    YANDEX_ACCESS_KEY = os.getenv('YANDEX_ACCESS_KEY', '')
    YANDEX_SECRET_KEY = os.getenv('YANDEX_SECRET_KEY', '')
    YANDEX_BUCKET = os.getenv('YANDEX_BUCKET', 'test-bucket')
    
    # Настройки тестовых данных
    TEST_FILE_CONTENT = b"Test content for Yandex Cloud S3 module testing"
    TEST_FILE_NAME = "test_file.txt"
    
    @classmethod
    def has_real_credentials(cls):
        """Проверяет наличие реальных учетных данных."""
        return bool(cls.YANDEX_ACCESS_KEY and cls.YANDEX_SECRET_KEY)
    
    @classmethod
    def skip_if_no_credentials(cls):
        """Декоратор для пропуска тестов без учетных данных."""
        import unittest
        def decorator(func):
            if not cls.has_real_credentials():
                return unittest.skip("Требуются реальные учетные данные Yandex Cloud")(func)
            return func
        return decorator