import requests
import json
import matplotlib.pyplot as plt
from PIL import Image
from matplotlib import patches
from io import BytesIO
subscription_key = "0001d91249d94b83b6eae6973496d0e4"
assert subscription_key
face_api_url = 'https://canadacentral.api.cognitive.microsoft.com/face/v1.0/detect'

# Currently pulls images from network - must make this local
image_url = 'https://amp.businessinsider.com/images/506f1690eab8eaef04000004-750-563.jpg'

headers = {'Ocp-Apim-Subscription-Key': subscription_key}
params = {
    'returnFaceAttributes':'emotion'
}
data = {'url': image_url}
response = requests.post(face_api_url, params=params, headers=headers, json=data)
faces = 'response.json()'