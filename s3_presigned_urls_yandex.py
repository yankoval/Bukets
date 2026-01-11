#!/usr/bin/env python3
"""
Модуль для работы с подписанными URL в Yandex Cloud Object Storage.
Адаптирован под специфичные требования Yandex Cloud API.
"""

import boto3
import requests
import argparse
import logging
import sys
import os
import json
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any
from botocore.exceptions import ClientError, NoCredentialsError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YandexS3PresignedURLManager:
    """Класс для управления подписанными URL в Yandex Cloud Storage."""

    def __init__(
            self,
            endpoint_url: str = "https://storage.yandexcloud.net/",
            region_name: str = "ru-central1",
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None
    ):
        """
        Инициализация клиента для Yandex Cloud.

        Важно: Yandex Cloud требует указания region_name.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            logger.info(f"Клиент инициализирован для Yandex Cloud: {endpoint_url}")

            # Проверяем доступность сервиса
            self._test_connection()

        except NoCredentialsError:
            logger.error("Не найдены учетные данные Yandex Cloud.")
            logger.info("Установите переменные окружения:")
            logger.info("  AWS_ACCESS_KEY_ID=your_yandex_key_id")
            logger.info("  AWS_SECRET_ACCESS_KEY=your_yandex_secret_key")
            logger.info("Или передайте ключи через --access-key и --secret-key")
            raise
        except Exception as e:
            logger.error(f"Ошибка инициализации клиента: {e}")
            raise

    def _test_connection(self) -> bool:
        """Проверка подключения к Yandex Cloud Storage."""
        try:
            # Пытаемся получить список бакетов
            response = self.s3_client.list_buckets()
            logger.info(f"Подключение успешно. Бакетов: {len(response.get('Buckets', []))}")
            return True
        except Exception as e:
            logger.warning(f"Не удалось получить список бакетов: {e}")
            logger.info("Продолжаем работу...")
            return False

    def create_presigned_post_url(
            self,
            bucket_name: str,
            object_name: str,
            expiration: int = 3600,
            max_size_mb: int = 10,
            content_type: Optional[str] = None,
            acl: str = "private"
    ) -> Optional[Dict]:
        """
        Создает подписанный URL для загрузки через POST.

        ВАЖНО: Yandex Cloud НЕ поддерживает поле 'success_action_status'!
        """
        try:
            # Условия политики (Yandex Cloud требует точного соответствия)
            conditions: List[Any] = [
                ["content-length-range", 1, max_size_mb * 1024 * 1024],
                {"key": object_name},  # Точное совпадение ключа
                {"bucket": bucket_name}  # Точное совпадение бакета
            ]

            # Базовые поля (только необходимые для Yandex Cloud)
            fields = {
                'key': object_name,
                'bucket': bucket_name,
            }

            # Добавляем ACL если указан
            if acl:
                conditions.append({"acl": acl})
                fields['acl'] = acl

            # Добавляем Content-Type если указан
            if content_type:
                # Для Yandex Cloud используем точное совпадение
                conditions.append({"Content-Type": content_type})
                fields['Content-Type'] = content_type

            # Генерация подписанного POST-запроса
            # ВАЖНО: не добавляем success_action_status!
            response = self.s3_client.generate_presigned_post(
                Bucket=bucket_name,
                Key=object_name,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=expiration
            )

            # Добавляем bucket в ответ для удобства
            response['bucket'] = bucket_name
            response['object'] = object_name

            logger.info(f"Создан POST URL для {bucket_name}/{object_name}, "
                        f"срок: {expiration} сек, макс. размер: {max_size_mb}MB")

            # Логируем поля для отладки
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Поля формы: {json.dumps(response['fields'], indent=2)}")
                logger.debug(f"Условия политики: {conditions}")

            return response

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"Ошибка ClientError при создании POST URL: {error_code}")
            logger.error(f"Подробности: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при создании POST URL: {e}")
            return None

    def upload_file_via_presigned_post(
            self,
            presigned_data: Dict,
            file_path: str,
            content_type: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Загружает файл через подписанный POST URL.

        Args:
            presigned_data: Данные из create_presigned_post_url()
            file_path: Путь к локальному файлу
            content_type: MIME-тип файла

        Returns:
            Кортеж (успех, сообщение)
        """
        try:
            # Проверка файла
            if not os.path.exists(file_path):
                return False, f"Файл не найден: {file_path}"

            file_size = os.path.getsize(file_path)
            logger.info(f"Загрузка файла: {file_path} ({file_size} bytes)")

            # Подготовка данных формы
            form_data = presigned_data['fields'].copy()

            # Чтение файла
            with open(file_path, 'rb') as file:
                # Подготовка файла для отправки
                file_name = os.path.basename(file_path)

                # Определяем Content-Type
                final_content_type = content_type
                if not final_content_type:
                    # Пробуем определить по расширению
                    import mimetypes
                    final_content_type, _ = mimetypes.guess_type(file_path)
                    if not final_content_type:
                        final_content_type = 'application/octet-stream'

                # Создаем словарь файлов для requests
                files = {'file': (file_name, file, final_content_type)}

                # Логируем данные для отладки
                logger.debug(f"Отправка POST на: {presigned_data['url']}")
                logger.debug(f"Поля формы: {form_data}")
                logger.debug(f"Content-Type: {final_content_type}")

                # Отправка POST-запроса
                response = requests.post(
                    presigned_data['url'],
                    data=form_data,
                    files=files,
                    timeout=30  # Таймаут 30 секунд
                )

            # Анализ ответа
            logger.debug(f"Статус ответа: {response.status_code}")
            logger.debug(f"Заголовки ответа: {dict(response.headers)}")

            if response.status_code in [200, 201, 204]:
                success_msg = f"Файл успешно загружен. Статус: {response.status_code}"
                if response.text:
                    success_msg += f"\nОтвет сервера: {response.text[:200]}"
                logger.info(success_msg)
                return True, success_msg
            else:
                error_msg = f"Ошибка загрузки. Статус: {response.status_code}"
                if response.text:
                    error_msg += f"\nОтвет сервера: {response.text}"
                    # Парсим XML ошибку если есть
                    if '<?xml' in response.text:
                        try:
                            import xml.etree.ElementTree as ET
                            root = ET.fromstring(response.text)
                            code = root.find('Code')
                            message = root.find('Message')
                            if code is not None and message is not None:
                                error_msg += f"\nКод ошибки: {code.text}"
                                error_msg += f"\nСообщение: {message.text}"
                        except:
                            pass
                logger.error(error_msg)
                return False, error_msg

        except requests.exceptions.Timeout:
            error_msg = "Таймаут при загрузке файла"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Ошибка при загрузке файла: {e}"
            logger.error(error_msg)
            return False, error_msg

    def create_presigned_get_url(
            self,
            bucket_name: str,
            object_name: str,
            expiration: int = 3600
    ) -> Optional[str]:
        """
        Создает подписанный URL для скачивания файла (GET).
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': object_name
                },
                ExpiresIn=expiration
            )

            logger.info(f"Создан GET URL для {bucket_name}/{object_name}, "
                        f"срок: {expiration} сек")
            return url

        except ClientError as e:
            logger.error(f"Ошибка при создании GET URL: {e}")
            return None

    def download_file_via_presigned_url(
            self,
            presigned_url: str,
            output_path: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Скачивает файл по подписанному URL.
        """
        try:
            logger.info(f"Скачивание по URL: {presigned_url[:50]}...")

            # Загрузка с таймаутом и stream
            response = requests.get(presigned_url, stream=True, timeout=30)

            if response.status_code != 200:
                return False, f"Ошибка скачивания. Статус: {response.status_code}"

            # Определение имени файла
            if output_path:
                save_path = output_path
            else:
                # Извлекаем имя из URL или заголовков
                filename = None

                # Из заголовка Content-Disposition
                content_disp = response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disp:
                    import re
                    match = re.search(r'filename="([^"]+)"', content_disp)
                    if match:
                        filename = match.group(1)

                # Или из URL
                if not filename:
                    filename = presigned_url.split('?')[0].split('/')[-1] or 'downloaded_file'

                save_path = filename

            # Создаем директорию если нужно
            os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)

            # Сохранение файла с прогрессом
            total_size = 0
            chunk_size = 8192

            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
                        total_size += len(chunk)

            logger.info(f"Файл скачан: {save_path} ({total_size:,} bytes)")
            return True, f"Файл сохранен: {save_path} ({total_size:,} bytes)"

        except requests.exceptions.Timeout:
            error_msg = "Таймаут при скачивании файла"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Ошибка при скачивании файла: {e}"
            logger.error(error_msg)
            return False, error_msg


def main():
    """Основная функция для работы через командную строку."""

    parser = argparse.ArgumentParser(
        description='Работа с подписанными URL в Yandex Cloud Storage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Создать URL для загрузки
  %(prog)s --action generate --bucket my-bucket --key uploads/file.txt

  # Загрузить файл
  %(prog)s --action upload --bucket my-bucket --key uploads/file.txt --file local.txt

  # Скачать файл
  %(prog)s --action download --bucket my-bucket --key uploads/file.txt --output ./downloaded.txt

  # Создать URL с ограничениями
  %(prog)s --action generate --bucket my-bucket --key images/photo.jpg \\
           --content-type image/jpeg --max-size 5 --expiration 1800

  # Подробный режим для отладки
  %(prog)s --action generate --bucket my-bucket --key test.txt --verbose
        """
    )

    # Основные аргументы
    parser.add_argument('--action', required=True,
                        choices=['generate', 'upload', 'download'],
                        help='Действие: generate - создать URL, upload - загрузить, download - скачать')
    parser.add_argument('--bucket', required=True,
                        help='Имя бакета в Yandex Cloud Storage')
    parser.add_argument('--key', required=True,
                        help='Ключ объекта (путь в бакете)')

    # Дополнительные аргументы
    parser.add_argument('--file',
                        help='Путь к локальному файлу (для загрузки)')
    parser.add_argument('--max-size', type=int, default=10,
                        help='Максимальный размер файла в MB (по умолчанию: 10)')
    parser.add_argument('--content-type',
                        help='MIME-тип файла (например, image/jpeg)')
    parser.add_argument('--acl', default='private',
                        choices=['private', 'public-read', 'authenticated-read'],
                        help='ACL объекта (по умолчанию: private)')

    # Аргументы для скачивания
    parser.add_argument('--output',
                        help='Путь для сохранения скачанного файла')

    # Общие аргументы
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

    args = parser.parse_args()

    # Настройка уровня логирования
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        # Также включаем debug для boto3
        logging.getLogger('boto3').setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    try:
        # Инициализация менеджера
        manager = YandexS3PresignedURLManager(
            endpoint_url=args.endpoint,
            region_name=args.region,
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key
        )

        # Выполнение действия
        if args.action == 'generate':
            print("\n" + "=" * 60)
            print("СОЗДАНИЕ ПОДПИСАННОГО URL ДЛЯ YANDEX CLOUD")
            print("=" * 60)

            presigned_data = manager.create_presigned_post_url(
                bucket_name=args.bucket,
                object_name=args.key,
                expiration=args.expiration,
                max_size_mb=args.max_size,
                content_type=args.content_type,
                acl=args.acl
            )

            if presigned_data:
                print(f"\n✅ URL создан успешно!")
                print(f"\n📦 Бакет: {presigned_data.get('bucket', args.bucket)}")
                print(f"📁 Объект: {presigned_data.get('object', args.key)}")
                print(f"⏱  Действителен: {args.expiration} секунд")
                print(f"📏 Макс. размер: {args.max_size} MB")

                print(f"\n🌐 URL для загрузки:")
                print(f"  {presigned_data['url']}")

                print(f"\n📋 ОБЯЗАТЕЛЬНЫЕ ПОЛЯ ДЛЯ ФОРМЫ:")
                for key, value in presigned_data['fields'].items():
                    print(f"  • {key}: {value}")

                print(f"\n📝 ПРИМЕР HTML ФОРМЫ:")
                print(f"""<form action="{presigned_data['url']}" method="post" enctype="multipart/form-data">""")
                for key, value in presigned_data['fields'].items():
                    print(f'  <input type="hidden" name="{key}" value="{value}">')
                print(f'  <input type="file" name="file" required>')
                print(f'  <button type="submit">Загрузить файл</button>')
                print(f'</form>')

                print(f"\n⚡ Пример curl команды:")
                curl_fields = " ".join([f"-F '{k}={v}'" for k, v in presigned_data['fields'].items()])
                print(f"curl -X POST {curl_fields} -F 'file=@yourfile.ext' {presigned_data['url']}")
            else:
                print("\n❌ Ошибка: не удалось создать подписанный URL")
                sys.exit(1)

        elif args.action == 'upload':
            if not args.file:
                print("❌ Ошибка: для загрузки требуется указать --file")
                sys.exit(1)

            print(f"\n📤 ЗАГРУЗКА ФАЙЛА В YANDEX CLOUD")
            print(f"Файл: {args.file}")
            print(f"Цель: {args.bucket}/{args.key}")

            # Создаем URL
            presigned_data = manager.create_presigned_post_url(
                bucket_name=args.bucket,
                object_name=args.key,
                expiration=args.expiration,
                max_size_mb=args.max_size,
                content_type=args.content_type,
                acl=args.acl
            )

            if not presigned_data:
                print("❌ Ошибка: не удалось создать подписанный URL")
                sys.exit(1)

            # Загружаем файл
            print(f"\n⏳ Загрузка...")
            success, message = manager.upload_file_via_presigned_post(
                presigned_data=presigned_data,
                file_path=args.file,
                content_type=args.content_type
            )

            if success:
                print(f"\n✅ {message}")
            else:
                print(f"\n❌ {message}")
                sys.exit(1)

        elif args.action == 'download':
            print(f"\n📥 СКАЧИВАНИЕ ФАЙЛА ИЗ YANDEX CLOUD")
            print(f"Источник: {args.bucket}/{args.key}")

            # Создаем URL для скачивания
            presigned_url = manager.create_presigned_get_url(
                bucket_name=args.bucket,
                object_name=args.key,
                expiration=args.expiration
            )

            if not presigned_url:
                print("❌ Ошибка: не удалось создать URL для скачивания")
                sys.exit(1)

            # Определяем путь для сохранения
            output_path = args.output
            if not output_path:
                output_path = os.path.basename(args.key) or 'downloaded_file'

            # Скачиваем файл
            print(f"⏳ Скачивание в: {output_path}...")
            success, message = manager.download_file_via_presigned_url(
                presigned_url=presigned_url,
                output_path=output_path
            )

            if success:
                print(f"\n✅ {message}")
            else:
                print(f"\n❌ {message}")
                sys.exit(1)

        print()  # Пустая строка в конце

    except KeyboardInterrupt:
        print("\n\n⚠️  Операция прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()