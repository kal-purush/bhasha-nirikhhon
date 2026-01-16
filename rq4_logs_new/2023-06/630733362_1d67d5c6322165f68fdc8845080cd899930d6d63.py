import numpy as np
import streamlit as st
from easyocr import Reader
from PIL import Image
from streamlit import sidebar as sb
from supervision import Color, Point, draw_text

lang_dict = {
    'en': 'English',
    'vi': 'Vietnamese',
    'ja': 'Japanese',
    'ch_sim': 'Simplified Chinese',
    'ch_tra': 'Traditional Chinese',
    'abq': 'Abaza',
    'ady': 'Adyghe',
    'af': 'Afrikaans',
    'ang': 'Angika',
    'ar': 'Arabic',
    'as': 'Assamese',
    'ava': 'Avar',
    'az': 'Azerbaijani',
    'be': 'Belarusian',
    'bg': 'Bulgarian',
    'bh': 'Bihari',
    'bho': 'Bhojpuri',
    'bn': 'Bengali',
    'bs': 'Bosnian',
    'che': 'Chechen',
    'cs': 'Czech',
    'cy': 'Welsh',
    'da': 'Danish',
    'dar': 'Dargwa',
    'de': 'German',
    'es': 'Spanish',
    'et': 'Estonian',
    'fa': 'Persian (Farsi)',
    'fr': 'French',
    'ga': 'Irish',
    'gom': 'Goan Konkani',
    'hi': 'Hindi',
    'hr': 'Croatian',
    'hu': 'Hungarian',
    'id': 'Indonesian',
    'inh': 'Ingush',
    'is': 'Icelandic',
    'it': 'Italian',
    'kbd': 'Kabardian',
    'kn': 'Kannada',
    'ko': 'Korean',
    'ku': 'Kurdish',
    'la': 'Latin',
    'lbe': 'Lak',
    'lez': 'Lezghian',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'mah': 'Magahi',
    'mai': 'Maithili',
    'mi': 'Maori',
    'mn': 'Mongolian',
    'mr': 'Marathi',
    'ms': 'Malay',
    'mt': 'Maltese',
    'ne': 'Nepali',
    'new': 'Newari',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'oc': 'Occitan',
    'pi': 'Pali',
    'pl': 'Polish',
    'pt': 'Portuguese',
    'ro': 'Romanian',
    'ru': 'Russian',
    'rs_cyrillic': 'Serbian (cyrillic)',
    'rs_latin': 'Serbian (latin)',
    'sck': 'Nagpuri',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'sq': 'Albanian',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'tab': 'Tabassaran',
    'te': 'Telugu',
    'th': 'Thai',
    'tjk': 'Tajik',
    'tl': 'Tagalog',
    'tr': 'Turkish',
    'ug': 'Uyghur',
    'uk': 'Ukranian',
    'ur': 'Urdu',
    'uz': 'Uzbek',
}


def filter_by_vals(d: dict, place, text: str) -> list[int | str]:
    a = list(d.values())

    if place.checkbox(text):
        return [
            a.index(i) for i in place.multiselect(' ', a, label_visibility='collapsed')
        ]
    else:
        return list(d.keys())


file = sb.file_uploader('Upload image', type=['png', 'jpg', 'jpeg'])
if file:
    img = np.array(Image.open(file))
    st.image(img, use_column_width=True)

    langs = filter_by_vals(lang_dict, sb, 'Languages')

    if len(lang_dict) == len(lang_dict.keys()):
        langs = ['en']

    reader = Reader(langs)
    onlytext = reader.readtext(img, detail=0)
    st.write(' '.join(onlytext))

    result = reader.readtext(img)
    blank = np.ones_like(img) * 255
    for r in result:
        bbox, text, prob = r
        center = np.array(bbox).mean(axis=0).astype(int)
        draw_text(
            scene=img,
            text=text,
            text_anchor=Point(x=center[0], y=center[1]),
            background_color=Color.white(),
        )
        draw_text(
            scene=blank,
            text=text + f'{prob:.2f}',
            text_anchor=Point(x=center[0], y=center[1]),
        )

    st.image(img, use_column_width=True)
    st.image(blank, use_column_width=True)