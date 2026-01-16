import requests

from messenger.vk.VKAPIRequest import vk_requests_url, vk_upload_photos


def vk_send_text(user_id, text):
    url = vk_requests_url('messages.send', message=text, user_id=user_id)

    requests.get(url)

    return 'ok'


def vk_send_photo(user_id, file_name, message=''):
    photo = vk_upload_photos(user_id, file_name)

    attachment = 'photo{owner_id}_{picture_id}'.format(owner_id=photo['owner_id'], picture_id=photo['id'])

    url = vk_requests_url('messages.send', user_id=user_id, message=message, attachment=attachment)

    requests.get(url)

    return 'ok'