from flask import Flask, request, jsonify
import tltk
import re

app = Flask(__name__)

def add_diacritics(ch, tone):
  tone = int(tone)
  # print('ch:' + ch)
  if tone == 1:
    return ch + '̄'
  elif tone == 2:
    return ch + '̌'
  elif tone == 3:
    return ch + '̀'
  elif tone == 4:
    return ch + '̃'
  elif tone == 5:
    return ch + '́'

def t2i(t):
  new_list = []
  result = tltk.nlp.th2ipa(t)
  words = re.split('[. ]', result)

  for word in words:
    # print(word)
    if '<' in word or word == '':
      continue
    tone = word[-1]
    # print(word)
    r = re.search('[aioueɯᴐɤɛ]', word)
    matched_vowel = r.group(0)
    new_word = word[:-1]
    new_word = new_word.replace(matched_vowel, add_diacritics(matched_vowel, tone), 1)
    # print(new_word)
    new_word = new_word.replace('ː', ':')
    new_word = new_word.replace('ɛ', 'æ')
    new_word = new_word.replace('ʔ', '')
    new_word = new_word.replace('ɤ', 'ə')
    new_word = new_word.replace('ʰ', 'h')
    new_word = new_word.replace('ŋ', 'ng')

    if new_word[0] == 'j':
        new_word = 'y' + new_word[1:]
      
    if 'c' in new_word and 'ch' not in new_word:
      new_word = new_word.replace('c', 'j')

    if new_word[-1] == 'j':
      new_word = new_word[0:-1] + 'i'

    if 'ɯ' in new_word:
      new_word = new_word.replace('ɯ', 'eu')

    new_list.append(new_word)
  return ' '.join(new_list)

def words2i(s):
  result = []
  for w in re.split('[ \n]', s):
    # print('1')
    # print(w)
    print(t2i(w))
    result.append(t2i(w))
    # print(th_2_zh(w))
  return '\n'.join(result)

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    input_string = data.get('input_string')
    
    if not input_string:
        return jsonify({'error': 'No input string provided'}), 400
    
    output_string = words2i(input_string)
    return jsonify({'output': output_string})

if __name__ == '__main__':
    app.run(debug=True)