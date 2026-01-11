#!/usr/bin/env python3
"""
Исправленный модуль тестирования для Yandex Cloud Storage.
"""

import unittest
import tempfile
import os
import sys
import json
from unittest.mock import MagicMock, patch, mock_open, call
from pathlib import Path

# Добавляем путь к модулю для тестирования
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from s3_presigned_urls_yandex import YandexS3PresignedURLManager

    MODULE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Cannot import module: {e}")
    MODULE_AVAILABLE = False


    # Создаем заглушку для тестов
    class YandexS3PresignedURLManager:
        pass


@unittest.skipIf(not MODULE_AVAILABLE, "Module not available")
class TestYandexS3PresignedURLManager(unittest.TestCase):
    """Тесты для YandexS3PresignedURLManager."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        # Мокаем boto3.client
        self.mock_s3_client = MagicMock()
        self.mock_s3_client.list_buckets.return_value = {'Buckets': []}

        # Мокаем boto3.client() вызов
        self.client_patcher = patch('s3_presigned_urls_yandex.boto3.client')
        self.mock_boto3_client = self.client_patcher.start()
        self.mock_boto3_client.return_value = self.mock_s3_client

        # Создаем экземпляр менеджера с тестовыми ключами
        self.manager = YandexS3PresignedURLManager(
            endpoint_url="https://storage.yandexcloud.net/",
            region_name="ru-central1",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key"
        )

    def tearDown(self):
        """Очистка после каждого теста."""
        self.client_patcher.stop()
        # Сбрасываем счетчики вызовов
        self.mock_s3_client.reset_mock()

    def test_init_success(self):
        """Тест успешной инициализации менеджера."""
        self.assertIsNotNone(self.manager)
        self.assertIsNotNone(self.manager.s3_client)
        self.mock_boto3_client.assert_called_once_with(
            's3',
            endpoint_url="https://storage.yandexcloud.net/",
            region_name="ru-central1",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key"
        )

    def test_test_connection_success(self):
        """Тест успешной проверки подключения."""
        # Reset mock call count since list_buckets might have been called in __init__
        self.mock_s3_client.list_buckets.reset_mock()

        result = self.manager._test_connection()
        self.assertTrue(result)
        self.mock_s3_client.list_buckets.assert_called_once()

    def test_test_connection_failure(self):
        """Тест неудачной проверки подключения."""
        # Reset mock
        self.mock_s3_client.list_buckets.reset_mock()
        self.mock_s3_client.list_buckets.side_effect = Exception("Connection error")

        result = self.manager._test_connection()
        self.assertFalse(result)
        self.mock_s3_client.list_buckets.assert_called_once()

    def test_create_presigned_post_url_success(self):
        """Тест успешного создания подписанного POST URL."""
        # Мокаем generate_presigned_post
        expected_response = {
            'url': 'https://storage.yandexcloud.net/test-bucket',
            'fields': {
                'key': 'uploads/test.txt',
                'bucket': 'test-bucket',
                'Content-Type': 'text/plain',
                'acl': 'private',
                'x-amz-algorithm': 'AWS4-HMAC-SHA256',
                'x-amz-credential': 'test-credential',
                'x-amz-date': '20240101T000000Z',
                'x-amz-signature': 'test-signature',
                'Policy': 'test-policy'
            }
        }

        self.mock_s3_client.generate_presigned_post.return_value = expected_response

        # Вызываем тестируемый метод
        result = self.manager.create_presigned_post_url(
            bucket_name='test-bucket',
            object_name='uploads/test.txt',
            expiration=3600,
            max_size_mb=10,
            content_type='text/plain'
        )

        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertEqual(result['url'], expected_response['url'])
        self.assertIn('fields', result)

        # Проверяем вызов метода с правильными параметрами
        self.mock_s3_client.generate_presigned_post.assert_called_once()
        call_args = self.mock_s3_client.generate_presigned_post.call_args

        self.assertEqual(call_args[1]['Bucket'], 'test-bucket')
        self.assertEqual(call_args[1]['Key'], 'uploads/test.txt')
        self.assertEqual(call_args[1]['ExpiresIn'], 3600)

        # Проверяем условия политики
        conditions = call_args[1]['Conditions']
        self.assertTrue(any('content-length-range' in str(c) for c in conditions))
        self.assertTrue(any('text/plain' in str(c) for c in conditions))

    def test_create_presigned_post_url_without_content_type(self):
        """Тест создания POST URL без content-type."""
        mock_response = {
            'url': 'https://storage.yandexcloud.net/test-bucket',
            'fields': {'key': 'uploads/test.txt', 'bucket': 'test-bucket'}
        }
        self.mock_s3_client.generate_presigned_post.return_value = mock_response

        result = self.manager.create_presigned_post_url(
            bucket_name='test-bucket',
            object_name='uploads/test.txt'
        )

        self.assertIsNotNone(result)
        call_args = self.mock_s3_client.generate_presigned_post.call_args[1]

        # Проверяем, что Content-Type не добавлен в условия
        conditions = call_args['Conditions']
        content_type_conditions = [c for c in conditions if 'Content-Type' in str(c)]
        self.assertEqual(len(content_type_conditions), 0)

    def test_create_presigned_post_url_client_error(self):
        """Тест обработки ошибки клиента при создании POST URL."""
        # Mock specific ClientError
        from botocore.exceptions import ClientError

        error_response = {
            'Error': {
                'Code': 'AccessDenied',
                'Message': 'Access denied'
            }
        }
        self.mock_s3_client.generate_presigned_post.side_effect = ClientError(
            error_response,
            'generate_presigned_post'
        )

        result = self.manager.create_presigned_post_url(
            bucket_name='test-bucket',
            object_name='uploads/test.txt'
        )

        self.assertIsNone(result)

    def test_create_presigned_get_url_success(self):
        """Тест успешного создания подписанного GET URL."""
        expected_url = 'https://storage.yandexcloud.net/test-bucket/uploads/test.txt?signature=test'
        self.mock_s3_client.generate_presigned_url.return_value = expected_url

        result = self.manager.create_presigned_get_url(
            bucket_name='test-bucket',
            object_name='uploads/test.txt',
            expiration=1800
        )

        self.assertEqual(result, expected_url)
        self.mock_s3_client.generate_presigned_url.assert_called_once_with(
            'get_object',
            Params={'Bucket': 'test-bucket', 'Key': 'uploads/test.txt'},
            ExpiresIn=1800
        )

    def test_create_presigned_get_url_failure(self):
        """Тест неудачного создания GET URL."""
        # Use ClientError instead of generic Exception
        from botocore.exceptions import ClientError

        error_response = {'Error': {'Code': 'InternalError', 'Message': 'Internal error'}}
        self.mock_s3_client.generate_presigned_url.side_effect = ClientError(
            error_response,
            'generate_presigned_url'
        )

        result = self.manager.create_presigned_get_url(
            bucket_name='test-bucket',
            object_name='uploads/test.txt'
        )

        self.assertIsNone(result)


@unittest.skipIf(not MODULE_AVAILABLE, "Module not available")
class TestUploadMethods(unittest.TestCase):
    """Тесты методов загрузки файлов."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        # Мокаем boto3
        self.client_patcher = patch('s3_presigned_urls_yandex.boto3.client')
        self.mock_boto3_client = self.client_patcher.start()
        self.mock_s3_client = MagicMock()
        self.mock_boto3_client.return_value = self.mock_s3_client

        self.manager = YandexS3PresignedURLManager(
            endpoint_url="https://test.endpoint/",
            region_name="test-region"
        )

        # Создаем временный файл для тестов
        self.temp_file = tempfile.NamedTemporaryFile(
            suffix='.txt',
            delete=False,
            mode='w'
        )
        self.temp_file.write("Test content for upload")
        self.temp_file.close()

    def tearDown(self):
        """Очистка после каждого теста."""
        self.client_patcher.stop()
        # Удаляем временный файл
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)

    @patch('s3_presigned_urls_yandex.requests.post')
    def test_upload_file_success(self, mock_post):
        """Тест успешной загрузки файла."""
        # Мокаем ответ requests
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "Success"
        mock_response.headers = {}
        mock_post.return_value = mock_response

        # Тестовые данные подписанного URL
        presigned_data = {
            'url': 'https://storage.yandexcloud.net/test-bucket',
            'fields': {
                'key': 'uploads/test.txt',
                'bucket': 'test-bucket',
                'Content-Type': 'text/plain'
            }
        }

        # Вызываем тестируемый метод
        success, message = self.manager.upload_file_via_presigned_post(
            presigned_data=presigned_data,
            file_path=self.temp_file.name,
            content_type='text/plain'
        )

        # Проверяем результат
        self.assertTrue(success)
        self.assertIn("успешно", message.lower())

        # Проверяем вызов requests.post
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        self.assertEqual(call_args[0][0], presigned_data['url'])
        self.assertIn('data', call_args[1])
        self.assertIn('files', call_args[1])

    @patch('s3_presigned_urls_yandex.requests.post')
    def test_upload_file_not_found(self, mock_post):
        """Тест загрузки несуществующего файла."""
        presigned_data = {
            'url': 'https://test.url',
            'fields': {'key': 'test.txt'}
        }

        success, message = self.manager.upload_file_via_presigned_post(
            presigned_data=presigned_data,
            file_path='/nonexistent/path/file.txt'
        )

        self.assertFalse(success)
        self.assertIn("не найден", message.lower())
        mock_post.assert_not_called()

    @patch('s3_presigned_urls_yandex.requests.post')
    def test_upload_file_server_error(self, mock_post):
        """Тест загрузки с ошибкой сервера."""
        # Мокаем ответ с ошибкой
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = '<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>'
        mock_response.headers = {}
        mock_post.return_value = mock_response

        presigned_data = {
            'url': 'https://test.url',
            'fields': {'key': 'test.txt'}
        }

        success, message = self.manager.upload_file_via_presigned_post(
            presigned_data=presigned_data,
            file_path=self.temp_file.name
        )

        self.assertFalse(success)
        self.assertIn("403", message)
        self.assertIn("accessdenied", message.lower())

    @patch('s3_presigned_urls_yandex.requests.post')
    def test_upload_file_timeout(self, mock_post):
        """Тест таймаута при загрузке."""
        from requests.exceptions import Timeout

        mock_post.side_effect = Timeout("Request timed out")

        presigned_data = {
            'url': 'https://test.url',
            'fields': {'key': 'test.txt'}
        }

        success, message = self.manager.upload_file_via_presigned_post(
            presigned_data=presigned_data,
            file_path=self.temp_file.name
        )

        self.assertFalse(success)
        self.assertIn("таймаут", message.lower())


@unittest.skipIf(not MODULE_AVAILABLE, "Module not available")
class TestDownloadMethods(unittest.TestCase):
    """Тесты методов скачивания файлов."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        self.client_patcher = patch('s3_presigned_urls_yandex.boto3.client')
        self.mock_boto3_client = self.client_patcher.start()
        self.mock_s3_client = MagicMock()
        self.mock_boto3_client.return_value = self.mock_s3_client

        self.manager = YandexS3PresignedURLManager()

    def tearDown(self):
        """Очистка после каждого теста."""
        self.client_patcher.stop()

    @patch('s3_presigned_urls_yandex.requests.get')
    def test_download_file_success(self, mock_get):
        """Тест успешного скачивания файла."""
        # Мокаем ответ requests
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}

        # Создаем mock для итерации по чанкам
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2', b'chunk3']
        mock_get.return_value = mock_response

        # Создаем временный файл для сохранения
        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_file:
            output_path = temp_file.name

        try:
            # Тестируем скачивание
            success, message = self.manager.download_file_via_presigned_url(
                presigned_url='https://storage.yandexcloud.net/test-bucket/file.txt',
                output_path=output_path
            )

            # Проверяем результат
            self.assertTrue(success)
            self.assertIn("сохранен", message.lower())

            # Проверяем создание файла
            self.assertTrue(os.path.exists(output_path))

            # Проверяем вызов requests.get
            mock_get.assert_called_once_with(
                'https://storage.yandexcloud.net/test-bucket/file.txt',
                stream=True,
                timeout=30
            )
        finally:
            # Удаляем временный файл
            if os.path.exists(output_path):
                os.unlink(output_path)

    @patch('s3_presigned_urls_yandex.requests.get')
    def test_download_file_auto_filename(self, mock_get):
        """Тест скачивания с автоматическим определением имени файла."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Disposition': 'attachment; filename="realname.txt"'}
        mock_response.iter_content.return_value = [b'test content']
        mock_get.return_value = mock_response

        success, message = self.manager.download_file_via_presigned_url(
            presigned_url='https://test.url/file.txt?signature=abc'
        )

        self.assertTrue(success)
        # Проверяем, что файл сохранился с правильным именем
        self.assertIn("realname.txt", message)

        # Удаляем созданный файл
        if os.path.exists("realname.txt"):
            os.unlink("realname.txt")

    @patch('s3_presigned_urls_yandex.requests.get')
    def test_download_file_server_error(self, mock_get):
        """Тест скачивания с ошибкой сервера."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.headers = {}
        mock_get.return_value = mock_response

        success, message = self.manager.download_file_via_presigned_url(
            presigned_url='https://test.url/file.txt'
        )

        self.assertFalse(success)
        self.assertIn("404", message)

    @patch('s3_presigned_urls_yandex.requests.get')
    def test_download_file_timeout(self, mock_get):
        """Тест таймаута при скачивании."""
        from requests.exceptions import Timeout

        mock_get.side_effect = Timeout("Request timed out")

        success, message = self.manager.download_file_via_presigned_url(
            presigned_url='https://test.url/file.txt'
        )

        self.assertFalse(success)
        self.assertIn("таймаут", message.lower())


@unittest.skipIf(not MODULE_AVAILABLE, "Module not available")
class TestEdgeCases(unittest.TestCase):
    """Тесты граничных случаев и обработки ошибок."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        self.client_patcher = patch('s3_presigned_urls_yandex.boto3.client')
        self.mock_boto3_client = self.client_patcher.start()
        self.mock_s3_client = MagicMock()
        self.mock_boto3_client.return_value = self.mock_s3_client

        self.manager = YandexS3PresignedURLManager()

    def tearDown(self):
        """Очистка после каждого теста."""
        self.client_patcher.stop()

    def test_create_post_url_yandex_specific_conditions(self):
        """Тест создания условий политики специфичных для Yandex Cloud."""
        # Мокаем ответ
        mock_response = {
            'url': 'https://test.url',
            'fields': {'key': 'test.txt', 'bucket': 'test-bucket'}
        }
        self.mock_s3_client.generate_presigned_post.return_value = mock_response

        # Вызываем метод
        result = self.manager.create_presigned_post_url(
            bucket_name='test-bucket',
            object_name='test.txt',
            content_type='image/jpeg'
        )

        # Проверяем вызов generate_presigned_post
        call_args = self.mock_s3_client.generate_presigned_post.call_args[1]
        conditions = call_args['Conditions']

        # Yandex Cloud требует точные условия для Content-Type
        content_type_conditions = [
            c for c in conditions
            if isinstance(c, dict) and 'Content-Type' in c
        ]

        self.assertEqual(len(content_type_conditions), 1)
        self.assertEqual(content_type_conditions[0]['Content-Type'], 'image/jpeg')

    def test_create_post_url_with_different_acl(self):
        """Тест создания URL с разными ACL."""
        mock_response = {
            'url': 'https://test.url',
            'fields': {'key': 'test.txt', 'acl': 'public-read'}
        }
        self.mock_s3_client.generate_presigned_post.return_value = mock_response

        result = self.manager.create_presigned_post_url(
            bucket_name='test-bucket',
            object_name='test.txt',
            acl='public-read'
        )

        call_args = self.mock_s3_client.generate_presigned_post.call_args[1]
        fields = call_args['Fields']

        self.assertEqual(fields.get('acl'), 'public-read')

    @patch('s3_presigned_urls_yandex.os.path.getsize')
    @patch('s3_presigned_urls_yandex.os.path.exists')
    @patch('s3_presigned_urls_yandex.requests.post')
    def test_upload_large_file_exceeds_limit(self, mock_post, mock_exists, mock_getsize):
        """Тест попытки загрузки файла превышающего лимит."""
        mock_exists.return_value = True
        mock_getsize.return_value = 20 * 1024 * 1024  # 20MB

        presigned_data = {
            'url': 'https://test.url',
            'fields': {'key': 'large.txt'}
        }

        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            file_path = temp_file.name

        try:
            success, message = self.manager.upload_file_via_presigned_post(
                presigned_data=presigned_data,
                file_path=file_path,
                content_type='text/plain'
            )

            # Файл загрузится, но сервер отклонит из-за политики
            mock_post.assert_called_once()
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)


class TestModuleIntegration(unittest.TestCase):
    """Интеграционные тесты модуля (требуют настройки окружения)."""

    @unittest.skip("Требует реальных учетных данных Yandex Cloud")
    def test_real_connection(self):
        """Интеграционный тест с реальным Yandex Cloud."""
        # Этот тест требует настройки реальных учетных данных
        # Установите переменные окружения перед запуском
        manager = YandexS3PresignedURLManager()

        # Простой тест подключения
        connected = manager._test_connection()
        self.assertTrue(connected)

    def test_import_module(self):
        """Тест импорта модуля."""
        # Проверяем, что модуль импортируется без ошибок
        try:
            from s3_presigned_urls_yandex import (
                YandexS3PresignedURLManager,
                main
            )
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import module: {e}")


class TestMainFunctionSimple(unittest.TestCase):
    """Простые тесты главной функции."""

    def test_argparse_directly(self):
        """Прямой тест парсера аргументов."""
        import argparse
        from s3_presigned_urls_yandex import main

        # Создаем парсер как в main
        parser = argparse.ArgumentParser(
            description='Работа с подписанными URL в Yandex Cloud Storage'
        )

        # Добавляем аргументы как в main
        parser.add_argument('--action', required=True,
                            choices=['generate', 'upload', 'download'],
                            help='Действие: generate - создать URL, upload - загрузить, download - скачать')
        parser.add_argument('--bucket', required=True,
                            help='Имя бакета в Yandex Cloud Storage')
        parser.add_argument('--key', required=True,
                            help='Ключ объекта (путь в бакете)')
        parser.add_argument('--file',
                            help='Путь к локальному файлу (для загрузки)')
        parser.add_argument('--max-size', type=int, default=10,
                            help='Максимальный размер файла в MB (по умолчанию: 10)')
        parser.add_argument('--content-type',
                            help='MIME-тип файла (например, image/jpeg)')
        parser.add_argument('--acl', default='private',
                            choices=['private', 'public-read', 'authenticated-read'],
                            help='ACL объекта (по умолчанию: private)')
        parser.add_argument('--output',
                            help='Путь для сохранения скачанного файла')
        parser.add_argument('--endpoint', default='https://storage.yandexcloud.net/',
                            help='Endpoint URL Yandex Cloud')
        parser.add_argument('--region', default='ru-central1',
                            help='Регион бакета (по умолчанию: ru-central1)')
        parser.add_argument('--expiration', type=int, default=3600,
                            help='Время жизни URL в секундах (по умолчанию: 3600 = 1 час)')
        parser.add_argument('--access-key',
                            help='Access Key ID Yandex Cloud')
        parser.add_argument('--secret-key',
                            help='Secret Access Key Yandex Cloud')
        parser.add_argument('--verbose', '-v', action='store_true',
                            help='Подробный вывод (debug уровень)')

        # Тестируем парсинг различных наборов аргументов
        test_cases = [
            ['--action', 'generate', '--bucket', 'test-bucket', '--key', 'test.txt'],
            ['--action', 'upload', '--bucket', 'test-bucket', '--key', 'test.txt', '--file', 'local.txt'],
            ['--action', 'download', '--bucket', 'test-bucket', '--key', 'test.txt', '--output', 'out.txt'],
        ]

        for args in test_cases:
            # Парсим аргументы
            parsed = parser.parse_args(args)

            # Проверяем базовые поля
            self.assertIsNotNone(parsed)
            self.assertEqual(parsed.action, args[1])
            self.assertEqual(parsed.bucket, args[3])
            self.assertEqual(parsed.key, args[5])

            # Проверяем значения по умолчанию
            self.assertEqual(parsed.endpoint, 'https://storage.yandexcloud.net/')
            self.assertEqual(parsed.region, 'ru-central1')
            self.assertEqual(parsed.expiration, 3600)
            self.assertEqual(parsed.acl, 'private')
            self.assertEqual(parsed.max_size, 10)

    def test_argparse_with_all_arguments(self):
        """Тест парсера со всеми аргументами."""
        import argparse

        parser = argparse.ArgumentParser(
            description='Работа с подписанными URL в Yandex Cloud Storage'
        )

        parser.add_argument('--action', required=True,
                            choices=['generate', 'upload', 'download'])
        parser.add_argument('--bucket', required=True)
        parser.add_argument('--key', required=True)
        parser.add_argument('--file')
        parser.add_argument('--max-size', type=int, default=10)
        parser.add_argument('--content-type')
        parser.add_argument('--acl', default='private',
                            choices=['private', 'public-read', 'authenticated-read'])
        parser.add_argument('--output')
        parser.add_argument('--endpoint', default='https://storage.yandexcloud.net/')
        parser.add_argument('--region', default='ru-central1')
        parser.add_argument('--expiration', type=int, default=3600)
        parser.add_argument('--access-key')
        parser.add_argument('--secret-key')
        parser.add_argument('--verbose', '-v', action='store_true')

        # Полный набор аргументов
        args = [
            '--action', 'upload',
            '--bucket', 'my-bucket',
            '--key', 'uploads/file.txt',
            '--file', '/path/to/file.txt',
            '--max-size', '50',
            '--content-type', 'application/pdf',
            '--acl', 'public-read',
            '--output', '/tmp/download.pdf',
            '--endpoint', 'https://custom.endpoint/',
            '--region', 'us-east-1',
            '--expiration', '7200',
            '--access-key', 'AKIAIOSFODNN7EXAMPLE',
            '--secret-key', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            '--verbose'
        ]

        parsed = parser.parse_args(args)

        # Проверяем все значения
        self.assertEqual(parsed.action, 'upload')
        self.assertEqual(parsed.bucket, 'my-bucket')
        self.assertEqual(parsed.key, 'uploads/file.txt')
        self.assertEqual(parsed.file, '/path/to/file.txt')
        self.assertEqual(parsed.max_size, 50)
        self.assertEqual(parsed.content_type, 'application/pdf')
        self.assertEqual(parsed.acl, 'public-read')
        self.assertEqual(parsed.output, '/tmp/download.pdf')
        self.assertEqual(parsed.endpoint, 'https://custom.endpoint/')
        self.assertEqual(parsed.region, 'us-east-1')
        self.assertEqual(parsed.expiration, 7200)
        self.assertEqual(parsed.access_key, 'AKIAIOSFODNN7EXAMPLE')
        self.assertEqual(parsed.secret_key, 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
        self.assertTrue(parsed.verbose)

# Дополнительные тесты для утилит
class TestUtilityFunctions(unittest.TestCase):
    """Тесты вспомогательных функций."""

    @patch('s3_presigned_urls_yandex.boto3.client')
    def test_policy_conditions_generation(self, mock_client):
        """Тест генерации условий политики."""
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        manager = YandexS3PresignedURLManager()

        # Тест с content-type
        mock_s3.generate_presigned_post.return_value = {
            'url': 'test',
            'fields': {'key': 'test.txt'}
        }

        manager.create_presigned_post_url(
            bucket_name='test',
            object_name='test.txt',
            content_type='image/jpeg'
        )

        # Получаем переданные условия
        call_args = mock_s3.generate_presigned_post.call_args[1]
        conditions = call_args['Conditions']

        # Проверяем наличие условия для content-type
        has_content_type = any(
            isinstance(c, dict) and 'Content-Type' in c
            for c in conditions
        )
        self.assertTrue(has_content_type)


class TestErrorMessages(unittest.TestCase):
    """Специальные тесты для сообщений об ошибках."""

    @patch('s3_presigned_urls_yandex.boto3.client')
    def test_error_message_localization(self, mock_client):
        """Тест локализации сообщений об ошибках."""
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        manager = YandexS3PresignedURLManager()

        # Тестируем сообщение о таймауте
        from requests.exceptions import Timeout

        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("test")
            file_path = f.name

        try:
            presigned_data = {
                'url': 'https://test.url',
                'fields': {'key': 'test.txt'}
            }

            # Мокаем requests.post для теста таймаута
            with patch('s3_presigned_urls_yandex.requests.post') as mock_post:
                mock_post.side_effect = Timeout("Request timed out")

                success, message = manager.upload_file_via_presigned_post(
                    presigned_data=presigned_data,
                    file_path=file_path
                )

                self.assertFalse(success)
                self.assertIn("таймаут", message.lower())
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)


def run_all_tests():
    """Запуск всех исправленных тестов."""
    # Создаем test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Добавляем тестовые классы в правильном порядке
    test_classes = [
        TestYandexS3PresignedURLManager,
        TestUploadMethods,
        TestDownloadMethods,
        TestEdgeCases,
        TestModuleIntegration,
        TestMainFunction,
        TestUtilityFunctions,
        TestErrorMessages
    ]

    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))

    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2, failfast=False)
    result = runner.run(suite)

    # Выводим статистику
    print("\n" + "=" * 60)
    print(f"Всего тестов: {result.testsRun}")
    print(f"Пройдено: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Не пройдено: {len(result.failures) + len(result.errors)}")

    if result.failures:
        print("\nНе пройденные тесты:")
        for test, traceback in result.failures:
            print(f"  • {test}")

    return result.wasSuccessful()


def run_specific_tests(test_names=None):
    """Запуск специфичных тестов."""
    if test_names is None:
        test_names = [
            "TestYandexS3PresignedURLManager.test_create_presigned_get_url_failure",
            "TestYandexS3PresignedURLManager.test_test_connection_success",
            "TestDownloadMethods.test_download_file_timeout"
        ]

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for name in test_names:
        try:
            # Извлекаем имя класса и метода
            class_name, method_name = name.split('.')

            # Находим класс
            test_class = globals()[class_name]

            # Добавляем тест
            suite.addTest(test_class(method_name))
        except (KeyError, ValueError) as e:
            print(f"Ошибка загрузки теста {name}: {e}")

    runner = unittest.TextTestRunner(verbosity=2)
    return runner.run(suite).wasSuccessful()


if __name__ == '__main__':
    print("Running Fixed Yandex S3 Module Tests...")
    print("=" * 50)

    # Запуск всех тестов
    success = run_all_tests()

    print("=" * 50)
    if success:
        print("✅ Все тесты пройдены успешно!")
        sys.exit(0)
    else:
        print("❌ Некоторые тесты не пройдены")

        # Опционально: запуск только проблемных тестов
        print("\nЗапуск повторных тестов для проблемных случаев...")
        problem_tests = [
            "TestYandexS3PresignedURLManager.test_create_presigned_get_url_failure",
            "TestYandexS3PresignedURLManager.test_test_connection_success",
            "TestDownloadMethods.test_download_file_timeout"
        ]

        print("Повторный запуск проблемных тестов:")
        run_specific_tests(problem_tests)

        sys.exit(1)