import os

from tests.base_test import BaseTest


class TestFileFsRepo(BaseTest):
    def test_should_save_file(
        self,
        file_fs_repo,
        fs_config,
        create_test_dir,
        sample_file,
        patch_uuid,
        cleanup_files,
    ):
        result = file_fs_repo.save("text/csv", sample_file)

        assert result == (
            os.path.join(fs_config.file_storage_path, "some-uuid-1.csv"),
            25,
        )
        assert os.path.exists(result[0])
        assert os.path.getsize(result[0]) == 25

    def test_should_save_pkl_file(
        self,
        file_fs_repo,
        fs_config,
        create_test_dir,
        sample_file,
        patch_uuid,
        cleanup_files,
    ):
        result = file_fs_repo.save("application/x-pickle", sample_file)

        assert result == (
            os.path.join(fs_config.file_storage_path, "some-uuid-1.pkl"),
            25,
        )
        assert os.path.exists(result[0])
        assert os.path.getsize(result[0]) == 25

    def test_should_get_file_info(
        self,
        file_fs_repo,
        fs_config,
        sample_file,
        create_test_dir,
        patch_uuid,
        cleanup_files,
    ):
        file_path, size = file_fs_repo.save("text/csv", sample_file)
        assert os.path.basename(file_path) == "some-uuid-1.csv"

        info_file_path, info_size = file_fs_repo.get_file_info("some-uuid-1.csv")
        assert info_file_path == file_path
        assert info_size == size

    def test_should_get_file_info_returns_none(self, file_fs_repo):
        file_info = file_fs_repo.get_file_info("no-such-file")
        assert file_info is None