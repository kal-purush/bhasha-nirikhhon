import urllib.request

def download_image(url, file_name):
    full_path = 'images/' + file_name + '.jpg'
    urllib.request.urlretrieve(url, full_path)

url = input('Enter image URL: ')
file_name = input('Enter a name to save the file as: ')

download_image(url, file_name)

# Example
# urllib.request.urlretrieve('https://example.com/image.jpg', 'images/myimage.jpg')