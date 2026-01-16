import uuid

import pytest
import shortuuid

from src.models import FileRecord
from src.repositories.file_fs_repo import FileFsRepo
from src.repositories.file_record_repo import FileRecordRepo
from tests.fixtures.db_fixtures import DbTest
from tests.fixtures.file_fixtures import FileFixtures


class BaseTest(DbTest, FileFixtures):

    @pytest.fixture
    def file_record_repo(self, db_session):
        return FileRecordRepo(db_session)

    @pytest.fixture
    def file_fs_repo(self):
        return FileFsRepo()

    @pytest.fixture
    def patch_uuid(self, monkeypatch):
        def uuid_gen():
            i = 1
            while True:
                yield i
                i += 1

        gen = uuid_gen()
        monkeypatch.setattr(uuid, "uuid4", lambda: f"some-uuid-{next(gen)}")

    @pytest.fixture
    def patch_shortuuid(self, monkeypatch):
        def web_id_gen():
            i = 1
            while True:
                yield f"web-id-{i}"
                i += 1

        gen = web_id_gen()

        def web_id():
            return next(gen)

        monkeypatch.setattr(shortuuid, "uuid", web_id)

    @pytest.fixture
    def cleanup_all(self, cleanup_file_records):
        pass

    @pytest.fixture
    def cleanup_file_records(self, file_record_repo):
        yield
        with file_record_repo.session():
            file_record_repo.query(FileRecord).delete()