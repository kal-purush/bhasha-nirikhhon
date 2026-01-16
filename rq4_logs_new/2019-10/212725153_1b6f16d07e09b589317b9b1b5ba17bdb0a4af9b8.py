# -*- coding:utf-8 -*-

import http.server
from gensim.models.word2vec import Word2Vec
import json
import urllib.request
import requests

# word2vecの読み込み
model_path = './learning/word2vec_model/word2vec.gensim.model'
model = Word2Vec.load(model_path)
with open('./learning/tweet_data/typical_words.json', 'r') as f:
  data_list = json.load(f)

class EmojiHandler(http.server.BaseHTTPRequestHandler):
  def do_POST(self):
    content_len = int(self.headers.get('content-length'))
    requestBody = json.load(self.rfile.read(content_len).decode('utf-8'))

    self.send_response(200)
    self.send_header('Content-type', 'aplication/json')
    self.send_header('Access-Control-Allow-Origin', '*')
    self.end_headers()

    # ここから評価
    url = 'https://labs.goo.ne.jp/api/morph'
    data = {
      'app_id': '58e6499c26849459065defb21ba415df2958862d0e2fbbc5fc7bc69f32a09ca5',
      'sentence': requestBody['sentence'],
      'pos_filter': '名詞|形容詞語幹|連用詞|連体詞|独立詞'
    }
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    r = requests.post(
      url=url,
      data=urllib.parse.urlencode(data),
      headers=headers
    )
    word_list = json.loads(r.text)['word_list'][0]

    emoji_num = 8
    rank_list = [[i, 0] for i in range(emoji_num)]
    for input_word in word_list:
      for data_word in data_list:
        try:
          sim = model.similarity(input_word[0], data_word[0])
          if sim > rank_list[data_word[1]][1]:
            rank_list[data_word[1]][1] = sim
        except KeyError as error:
          pass

    rank_list.sort(key=lambda x: x[1], reverse=True)
    # ここまで評価

    self.wfile.write(json.dumps([x[1] for x in rank_list]).encode('utf-8'))

with http.server.HTTPServer(('', 8000), EmojiHandler) as server:
  print('server runing...')
  server.serve_forever()
