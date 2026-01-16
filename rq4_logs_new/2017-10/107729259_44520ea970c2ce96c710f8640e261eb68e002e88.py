from .sdk.sdk.adminpanel import AdminPanel


class SelectelAdminPanel:

    def __init__(self, db):
        self._db = db

    def vpc_auth(self, chat_id, token):
        table = self._db['adminpanel_tokens']

        self._sdk = AdminPanel(token)

        table.insert_one({
            'chat_id': chat_id,
            'token': token
        })

    def auth_by_chat_id(self, chat_id):

        if hasattr(self, '_sdk'):
            return

        table = self._db['adminpanel_tokens']

        saved_user = table.find_one({'chat_id': chat_id})

        if saved_user:
            self._sdk = AdminPanel(saved_user['token'])

    def get_balance(self):

        if not hasattr(self, '_sdk'):
            return

        return self._sdk.get_balance()

    def get_user_info(self):

        if not hasattr(self, '_sdk'):
            return

        return self._sdk.get_user_info()