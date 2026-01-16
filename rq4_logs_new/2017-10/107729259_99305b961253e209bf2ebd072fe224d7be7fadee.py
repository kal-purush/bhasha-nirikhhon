from .sdk.sdk.cloudstorage import CloudStorage

class SelectelCloudStorage:

    def __init__(self, db):
        self.db = db

    def storage_auth(self, chat_id, user, password):

        table = self.db['cloudstorage_tokens']

        self.sdk = CloudStorage(user, password)

        table.insert_one({
            'user': user,
            'chat_id': chat_id,
            'token': self.sdk.auth_token,
            'storage_url': self.sdk.storage_url
        })

    def auth_by_chat_id(self, chat_id):

        if hasattr(self, 'sdk'):
            return

        table = self.db['cloudstorage_tokens']

        saved_user = table.find_one({'chat_id': chat_id})

        if saved_user:
            self.sdk = CloudStorage(saved_user['token'])
            self.sdk.set_storage_url(saved_user['storage_url'])

    def create_container(self, name, type='private'):

        if not hasattr(self, 'sdk'):
            return

        self.sdk.new_container(name, type)

    def get_containers_list(self):

        if not hasattr(self, 'sdk'):
            return

        return self.sdk.containers_list()

    def get_files_list(self, container, limit=None, marker=None, format='json'):

        if not hasattr(self, 'sdk'):
            return

        return self.sdk.get_files(container, limit=limit, marker=marker, format=format)

    def upload_file(self, container, file_name, file):

        if not hasattr(self, 'sdk'):
            return

        self.sdk.upload(container, file_name, file)

    def download_file(self, container, file_name):

        if not hasattr(self, 'sdk'):
            return

        return self.sdk.download(container, file_name)

    def delete_file(self, container, file_name):

        if not hasattr(self, 'sdk'):
            return

        self.sdk.delete(container, file_name)