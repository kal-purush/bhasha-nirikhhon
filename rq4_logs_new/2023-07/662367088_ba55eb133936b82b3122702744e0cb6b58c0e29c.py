from pathlib import Path
from PIL import Image
from google.cloud import vision
import os
import environ

env = environ.Env()
env.read_env('../.env')

def CharacterRec(image):
    key = env('CHARA_KEY')

    result = []
    img = Image.open(image)
    img.save('material/images/image.jpg')
    image = '../material/images/image.jpg'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key
    p = Path(__file__).parent / image
    client = vision.ImageAnnotatorClient()
    with p.open('rb') as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    response = client.text_detection(image=image)
    text = response.text_annotations[0].description
    text = text.splitlines()
    for text in text:
        if '氏名'in text:
            result.append(text[text.find('氏名')+3:])
        elif '住所' in text:
            result.append(text[text.find('住所')+3:])
        elif '日生' in text:
            result.append(text[:text.find('生')])
    print(result)
    os.remove('material/images/image.jpg')
    return result