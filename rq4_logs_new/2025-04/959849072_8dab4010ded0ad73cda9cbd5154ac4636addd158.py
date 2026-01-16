from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union

from requests.models import Response


class CloudClient(ABC):
    @abstractmethod
    def _initialize_headers(self) -> dict:
        """Инициализация заголовков с токеном доступа"""
        pass

    @abstractmethod
    def check_disk_access(self) -> Response:
        """Проверяет доступ к облаку"""
        pass

    @abstractmethod
    def _ensure_path_exists(self, remote_path: str) -> Union[bool, str]:
        """Рекурсивно создает путь к файлу/папке, если его не существует"""
        pass

    @abstractmethod
    def _path_exists(self, path: str) -> bool:
        """Проверяет, существует ли путь на Диске"""
        pass

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> Response:
        """Загружает файл в облако"""
        pass

    @abstractmethod
    def upload_folder(
        self, local_folder: str, remote_folder: str
    ) -> List[Response]:
        """Загружает папку в облако"""
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> Response:
        """Скачивает файл из облака"""
        pass

    @abstractmethod
    def list_files(
        self, remote_path: str = ""
    ) -> Tuple[Response, Optional[list]]:
        """Список файлов в облаке"""
        pass