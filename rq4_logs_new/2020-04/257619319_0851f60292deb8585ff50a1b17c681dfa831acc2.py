from pprint import pprint
from urllib.parse import urlencode
import requests
import json

def solitary_group(user1, maxpeople=0):
    #Получение id пользователя - удовлетворение условия "программа должна одинаково запускаться с ID и ника
    friend_count = 0
    response_id = requests.get(
        'https://api.vk.com/method/users.get',
        params={
            'access_token': TOKEN,
            'user_ids': user1,
            'v': 5.103
        }
    )
    #Получение списка друзей
    response_friends_list = requests.get(
        'https://api.vk.com/method/friends.get',
        params={
            'access_token': TOKEN,
            'user_id': response_id.json()['response'][0]['id'],
            'v': 5.103
        }
    )
    #Получение списка групп целевого пользователя и составление списка словарей групп
    response_user_groups = requests.get(
        'https://api.vk.com/method/groups.get',
        params={
            'access_token': TOKEN,
            'user_id': user1,
            'extended': 1,
            # 'fields': 'name', не работает! Не забыть спросить, почему!
            'v': 5.103
        }
    )
    user_group_list = []
    for user_group in response_user_groups.json()['response']['items']:
        user_group_list.append({'id': user_group['id'], 'name': user_group['name'], 'friends_with': 0})
        #словарь группы содержит id и имя для удобства отладки; кроме того, есть индекс
    #получение групп друзей
    for friend in response_friends_list.json()['response']['items']:
        friend_count += 1
        friend_group_count = 0
        response_friend_groups = requests.get(
            'https://api.vk.com/method/groups.get',
            params={
                'access_token': TOKEN,
                'user_id': friend,
                'extended': 1,
                # 'fields': 'name', не работает! Не забыть спросить, почему!
                'v': 5.103
            }
        )
        # print(f'Friend №{friend_count}: Id - {friend};', end='')
        try:
            print(f' first group - {response_friend_groups.json()["response"]["items"][0]}')
            # поиск схожих групп и удаление неуникальных
            for friend_group in response_friend_groups.json()["response"]["items"]:
                friend_group_count += 1
                user_group_count = 0
                if friend_group_count > 1000:
                    print('Ошибка: слишком много групп')
                    break
                for user_group in user_group_list:
                    user_group_count += 1
                    print(f'Friends: {friend_count}/{len(response_friends_list.json()["response"]["items"])}, '
                          f'friend groups: {friend_group_count}/{len(response_friend_groups.json()["response"]["items"])},'
                          f'user unique groups: {user_group_count}/{len(user_group_list)}')
                    if friend_group['id'] == user_group['id']:
                        user_group['friends_with'] += 1
                        if user_group['friends_with'] > maxpeople:
                            del(user_group_list[user_group_list.index(user_group)])

        #убирает падение программ от удаленных друзей или друзей без групп
        except KeyError:
            print('')
        except IndexError:
            print('')
    return user_group_list



APP_ID = 7423649
OAUTH_URL = 'https://oauth.vk.com/authorize'
OAUTH_PARAMS = {
    'client_id': APP_ID,
    'display': 'page',
    'scope': 'groups,friends',
    'response_type': 'token',
    'v': 5.52
}
# print('?'.join((OAUTH_URL, urlencode(OAUTH_PARAMS))))

TOKEN = '3714ec7679ee8e9352879d9f3a83f26ab7839cac1ed2421e2df7b99c4393c229b55b5e2758bd2c1d8b8c0'
user1_id = 4243253
user2_id = 6293784

pprint(solitary_group(6293784))