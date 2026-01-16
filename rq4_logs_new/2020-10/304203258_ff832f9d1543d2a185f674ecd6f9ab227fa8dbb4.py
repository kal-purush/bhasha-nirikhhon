import pickle
from pathlib import Path
from google.cloud import storage

from common import delete_dir_contents
from config import FILETYPE_SET


class Bucket:

    def __init__(self, bucket_name: str):
        self._storage_client = storage.Client()
        self.bucket = self._storage_client.bucket(bucket_name)
        self.bucket_name = bucket_name

    def upload_dir(self, rootdir: str):
        paths = [str(path) for path in Path(rootdir).rglob('*') if '.gitignore' not in path.stem]
        for path in paths:
            self.upload_to_bucket(source_filename=path.split('/')[-1])

    def download_dir(self, remote_dir: str):
        Path(remote_dir).mkdir(parents=True, exist_ok=True)
        delete_dir_contents(remote_dir)
        bucket_files = self.list_bucket_objects()
        for file in bucket_files:
            if file.name.startswith(remote_dir) and file.name.lower().endswith(tuple(FILETYPE_SET)):
                self.download_from_bucket(file.name, file.name)

    def upload_to_bucket(self, data=None, to_pickle=False, source_filename=None):
        """
        Uploads a picklable file to the gcloud bucket attached to this object
        :param source_filename: name of the file to be uploaded
        :param to_pickle: boolean indicator, default is False
        :param data: Any data to be uploaded
        :return: int code 1 for success, -1 for failure
        """

        try:
            if source_filename is None:
                source_filename = 'raw_images/input_data.pkl'
            if to_pickle and data is not None:
                with open(source_filename, 'wb') as f:
                    pickle.dump(data, f)
            dest_and_src_path = str(Path('raw_images', source_filename))

            blob = self.bucket.blob(dest_and_src_path)

            blob.upload_from_filename(dest_and_src_path)
            return 1
        except Exception as e:
            print(e)
            return -1

    def download_from_bucket(self, dest_file_name: str, source_blob_name: str):
        try:
            blob = self.bucket.blob(source_blob_name)
            blob.download_to_filename(dest_file_name)
            return 1
        except Exception as e:
            print(e)
            return -1

    # TODO should eventually clean up the processed_images directory too
    def clean_up_bucket(self) -> None:
        bucket_files = self.list_bucket_objects()
        for file in bucket_files:
            if file.name.startswith(('raw_images/', 'processed_images/')) \
                    and file.name.lower().endswith(tuple(FILETYPE_SET)):
                blob = self.bucket.blob(file.name)
                blob.delete()

    def list_bucket_objects(self) -> [str]:
        return list(self._storage_client.list_blobs(self.bucket_name))