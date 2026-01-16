import requests
from os import path
import xml.etree.ElementTree as ET
from icrawler.builtin import GoogleImageCrawler
import genanki
from pathlib import Path
from navertts import NaverTTS
import shutil
import argparse
import logging

CURRENT_DIR = Path(__file__).resolve().parent

MEDIA_DIR = CURRENT_DIR / 'media'
MEDIA_DIR.parent.mkdir(parents=True, exist_ok=True)

GOOGLE_CRAWLER = GoogleImageCrawler(storage={'root_dir': MEDIA_DIR}, log_level=logging.CRITICAL)

ANKI_MODEL = genanki.Model(
    1607392319,
    'Gen model',
    fields=[
        {'name': 'Korean'},
        {'name': 'Russian'},
        {'name': 'Image'},
        {'name': 'Sound'},
    ],
    templates=[
        {
            'name': 'Russian+Korean -> Korean',
            'qfmt': '{{Russian}}<br>{{Image}}{{type:Korean}}',
            'afmt': '{{FrontSide}}<hr id="answer"><br>{{Sound}}',
        },
        {
            'name': 'Korean+Russian -> Russian',
            'qfmt': '{{Korean}}<br>{{Sound}}{{type:Russian}}',
            'afmt': '{{FrontSide}}<hr id="answer">',
        },
    ],
    css="""
        .card {
            font-family: arial;
            font-size: 20px;
            text-align: center;
            color: black;
            background-color: white;
        }
    """
)

def word_dir(word):
    return MEDIA_DIR / word

def make_word_dir(word):
    word_dir(word).mkdir(parents=True, exist_ok=True)

def get_translation(word, api_key):
    res = requests.get('https://krdict.korean.go.kr/api/search', verify=CURRENT_DIR / 'cert.pem', params={
        'key': api_key, 
        'q': word,
        'translated': 'y',
        'trans_lang': '10',
        'lang': 10
    })
    assert res.status_code == 200
    root = ET.fromstring(res.text)
    return root.findall("./item/sense/translation/trans_word")[0].text

def download_image(word):
    GOOGLE_CRAWLER.crawl(keyword=word, max_num=1, overwrite=True)
    image_file = next(MEDIA_DIR.glob('000001.*'))
    new_name = word_dir(word) / f'{word}{image_file.suffix}'
    image_file.rename(new_name)
    return new_name

def make_sound(word):
    file = word_dir(word) / f'{word}.mp3'
    NaverTTS(word).save(file)
    return file

def make_note(word, api_key):
    make_word_dir(word)
    image_file = download_image(word)
    sound_file = make_sound(word)
    translation = get_translation(word, api_key)
    note = genanki.Note(model=ANKI_MODEL, fields=[word, translation, f'<img src="{image_file.name}">', f'[sound:{sound_file.name}]'])
    return (note, [image_file, sound_file])

def read_words_from_file(file_path):
    with open(args.words_list_file, 'r', encoding='UTF-8') as f:
        words = f.readlines()
    return words

def make_deck(deck_name, api_key, words):
    logger = logging.getLogger('anki-russian-korean')
    logger.setLevel(logging.INFO)

    deck = genanki.Deck(2059400110, deck_name)
    media = []
    for word in words:
        word = word.strip()
        if word == '':
            continue
        logger.info(f'Создаю карточку для слова {word}...')
        try:
            note, media_files = make_note(word, api_key)
            deck.add_note(note)
            media.extend(media_files)
            logger.info('Карточка для слова {word} была создана!')
        except:
            logger.error(f'Ошибка создания карточки для слова {word}')

    package = genanki.Package(deck)
    package.media_files = media
    package.write_to_file(CURRENT_DIR / f'{deck_name}.apkg')
    shutil.rmtree(MEDIA_DIR)

parser = argparse.ArgumentParser(
    prog='anki-russian-korean', 
    description='Генерирует колоду для Anki из корейских слов с переводом на русский, картинкой и произношением')
parser.add_argument('deck_name', help='Название колоды')
parser.add_argument('api_key', help='Ключ API krdict')
parser.add_argument('words_list_file', help='Путь к файлу с корейскими словами по одному слову на строке')
args = parser.parse_args()

make_deck(args.deck_name, args.api_key, read_words_from_file(args.words_list_file))