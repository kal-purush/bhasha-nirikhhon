import requests
import re
from typing import List

from fastapi import APIRouter


router = APIRouter(prefix="/api/v1")

ascii_map = {'&apos;': "'", '&quot;': '"'}


@router.get('/urban_dictionary/{query}')
def get_query(query: str) -> List[str]:
    url = f'https://www.urbandictionary.com/define.php?term={query}'
    data = get_content(url)
    info = parse(data)
    return info


def get_content(url: str):
    # req = requests.get(url)
    with open('urban.html', 'rb') as dd:
        content = dd.read()
    return content


def parse(raw_data: bytes):
    data = raw_data.decode('utf-8')
    mean = r'<div class="meaning">.*?.*?.*?</div>'
    exam = r'<div class="example">.*?.*?.*?</div>'

    meaning_list = re.findall(mean, data)
    exam_list = re.findall(exam, data)

    entries = []
    no = -1
    for x in exam_list:
        no += 1
        entry = {}
        entry['example'] = clean(x)
        entry['meaning'] = clean(meaning_list[no])
        entries.append(entry)
    
    return entries


def clean(data):
    fin = re.sub(r'<.*?.*?.*?>', '', data)
    strs = fin.splitlines()
    strs = '\r\n'.join(strs)
    for x in ascii_map:
        strs = strs.replace(x, ascii_map[x])
    return strs

get_query('')