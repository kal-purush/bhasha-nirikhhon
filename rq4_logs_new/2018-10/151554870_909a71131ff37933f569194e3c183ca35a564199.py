import requests

TOKEN = 'ed1271af9e8883f7a7c2cefbfddfcbc61563029666c487b2f71a5227cce0d1b533c4af4c5b888633c06ae'
USER_ID = '171691064'
USER_NAME = 'eshmargunov'
params = {'access_token': TOKEN,
          'v': '5.85'}

class UserVk:

    def __init__(self, user_id, user_name):
        self.user_id = user_id
        self.user_name = user_name

    @property
    def get_groups(self, user_id):
        pass


class MainUser(UserVk):

    def __init__(self, user_id, user_name, token):
        self.user_id = user_id
        self.user_name = user_name
        self.token = token

    @property
    def get_friends(self, user_id):
        url = 'https://api.vk.com/method/friends.get'
        user_params = {'user_id': user_id, 'fields': 'domain'}
        user_params.update(params)
        response = requests.get(url, user_params).json()
        return response['response']['items']



