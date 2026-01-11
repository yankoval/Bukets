#!/usr/bin/env python3
"""
Интеграционные тесты для реального Yandex Cloud Storage.
Требуют настройки учетных данных.
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path

# Добавляем путь к модулю
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_config import TestConfig
from s3_presigned_urls_yandex import YandexS3PresignedURLManager


@unittest.skipUnless(
    TestConfig.has_real_credentials(),
    "Требуются реальные учетные данные Yandex Cloud"
)
class IntegrationTests(unittest.TestCase):
    """Интеграционные тесты с реальным Yandex Cloud."""
    
    @classmethod
    def setUpClass(cls):
        """Настройка перед всеми тестами."""
        cls.manager = YandexS3PresignedURLManager(
            endpoint_url="https://storage.yandexcloud.net/",
            region_name="ru-central1",
            aws_access_key_id=TestConfig.YANDEX_ACCESS_KEY,
            aws_secret_access_key=TestConfig.YANDEX_SECRET_KEY
        )
        
        # Уникальный префикс для тестовых файлов
        import uuid
        cls.test_prefix = f"test_{uuid.uuid4().hex[:8]}_"
        
        # Создаем тестовый файл
        cls.test_file = tempfile.NamedTemporaryFile(
            suffix='.txt',
            delete=False,
            mode='wb'
        )
        cls.test_file.write(TestConfig.TEST_FILE_CONTENT)
        cls.test_file.close()
    
    @classmethod
    def tearDownClass(cls):
        """Очистка после всех тестов."""
        # Удаляем тестовый файл
        if os.path.exists(cls.test_file.name):
            os.unlink(cls.test_file.name)
    
    def test_01_connection(self):
        """Тест подключения к Yandex Cloud."""
        connected = self.manager._test_connection()
        self.assertTrue(connected)
    
    def test_02_create_presigned_url(self):
        """Тест создания подписанного URL."""
        object_name = f"{self.test_prefix}test_create.txt"
        
        presigned_data = self.manager.create_presigned_post_url(
            bucket_name=TestConfig.YANDEX_BUCKET,
            object_name=object_name,
            expiration=300,  # 5 минут
            max_size_mb=1,
            content_type='text/plain'
        )
        
        self.assertIsNotNone(presigned_data)
        self.assertIn('url', presigned_data)
        self.assertIn('fields', presigned_data)
        
        # Проверяем обязательные поля
        fields = presigned_data['fields']
        self.assertIn('key', fields)
        self.assertEqual(fields['key'], object_name)
    
    def test_03_upload_and_download(self):
        """Полный тест: загрузка и скачивание файла."""
        object_name = f"{self.test_prefix}test_upload.txt"
        
        # 1. Создаем подписанный URL для загрузки
        presigned_data = self.manager.create_presigned_post_url(
            bucket_name=TestConfig.YANDEX_BUCKET,
            object_name=object_name,
            expiration=600,
            max_size_mb=5,
            content_type='text/plain'
        )
        
        self.assertIsNotNone(presigned_data)
        
        # 2. Загружаем файл
        success, message = self.manager.upload_file_via_presigned_post(
            presigned_data=presigned_data,
            file_path=self.test_file.name,
            content_type='text/plain'
        )
        
        self.assertTrue(success, f"Upload failed: {message}")
        
        # 3. Создаем URL для скачивания
        download_url = self.manager.create_presigned_get_url(
            bucket_name=TestConfig.YANDEX_BUCKET,
            object_name=object_name,
            expiration=600
        )
        
        self.assertIsNotNone(download_url)
        
        # 4. Скачиваем файл
        with tempfile.NamedTemporaryFile(suffix='_downloaded.txt', delete=False) as temp_file:
            output_path = temp_file.name
        
        try:
            success, message = self.manager.download_file_via_presigned_url(
                presigned_url=download_url,
                output_path=output_path
            )
            
            self.assertTrue(success, f"Download failed: {message}")
            
            # 5. Проверяем содержимое
            with open(output_path, 'rb') as f:
                downloaded_content = f.read()
            
            self.assertEqual(downloaded_content, TestConfig.TEST_FILE_CONTENT)
        finally:
            # Очистка
            if os.path.exists(output_path):
                os.unlink(output_path)


if __name__ == '__main__':
    print("Running Integration Tests for Yandex Cloud...")
    print("=" * 60)
    
    if not TestConfig.has_real_credentials():
        print("⚠️  Предупреждение: реальные учетные данные не найдены")
        print("Установите переменные окружения:")
        print("  YANDEX_ACCESS_KEY, YANDEX_SECRET_KEY, YANDEX_BUCKET")
        print("Или создайте файл .env с этими переменными")
        print("\nЗапускаю только unit-тесты...")
    
    # Запускаем тесты
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(IntegrationTests)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    sys.exit(0 if result.wasSuccessful() else 1)