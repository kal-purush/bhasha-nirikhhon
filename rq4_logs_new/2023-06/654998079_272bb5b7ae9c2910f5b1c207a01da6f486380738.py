import requests
from config import MY_ID_RESUME, ACCESS_TOKEN


class Update:
    """ Класс обновления резюме через телеграм"""

    @classmethod
    def update_resume(cls):
        """ Поднятие резюме """

        URL = f'https://api.hh.ru/resumes/{MY_ID_RESUME}/publish/'

        header = {
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'User-Agent': 'api-test-agent'
        }

        response = requests.post(url=URL, headers=header)
        print('qqqqqqqqqqqqq')
        if response.status_code == 204:
            return 204
        if response.status_code == 403:
            raise Exception('Ошибка авторизации Access Token')
        return response.status_code