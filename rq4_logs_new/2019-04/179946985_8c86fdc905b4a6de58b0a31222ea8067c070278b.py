#!/usr/bin/env python

import io
import json
import uuid

from lib.ConsumeSite import ConsumeSite

MAPA_URL = "https://www.mapa.gob.es/"
GLOBAL_CONSUME_URL = MAPA_URL + "app/consumo-en-hogares/"

CONSUME_URLS = (GLOBAL_CONSUME_URL + "consulta04.asp",
#                GLOBAL_CONSUME_URL + "consulta05.asp",
#                GLOBAL_CONSUME_URL + "consulta06.asp",
#                GLOBAL_CONSUME_URL + "consulta10.asp",
#                GLOBAL_CONSUME_URL + "consulta11.asp",
               )

data = []
for site_url in CONSUME_URLS:
    consume_site = ConsumeSite()
    data.extend(consume_site.get_data(url = site_url))

file_name = "data-{0}.json".format(uuid.uuid4().hex)
with io.open(file_name, 'w', encoding='utf8') as json_file:
    json.dump(data, json_file, ensure_ascii=False)

    print("Data writed to file: {0}".format(file_name))
