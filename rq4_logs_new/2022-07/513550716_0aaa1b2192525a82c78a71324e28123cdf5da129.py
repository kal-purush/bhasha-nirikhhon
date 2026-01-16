import requests


def save_photos(image_url, name):
    response = requests.get(image_url)
    response.raise_for_status()
    with open(f'{name}.jpg', 'wb') as file:
        file.write(response.content)


def create_url(method):
    url = f"https://api.vk.com/method/{method}"
    return url