import vk_api
import time

login = input('Enter VK login: ')
password = input('Enter VK password: ')

app_id = '2685278'

try:
    try:
        vk_session = vk_api.VkApi(login, password, app_id=app_id)
        vk_session.auth()
    except vk_api.exceptions.BadPassword:
        print('Bad password!'); exit()
    except vk_api.exceptions.AuthError:
        vk_session = vk_api.VkApi(login, password, app_id=app_id, auth_handler = lambda: [input('Enter two-factor auth code: '), False])
        vk_session.auth()

    vk = vk_session.get_api()

    while vk.messages.getConversations()['count'] != 0:
        for chat in vk.messages.getConversations(count=200)['items']:
            vk.messages.deleteConversation(user_id=chat['conversation']['peer']['id'])
            time.sleep(1.5)

    print('Completed!')
except KeyboardInterrupt:
    print('Goodbye :D'); exit()