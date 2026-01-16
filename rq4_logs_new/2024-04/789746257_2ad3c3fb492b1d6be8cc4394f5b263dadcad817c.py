import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import json
from flask import Flask, request, Response
from flask_restful import Api, Resource
from utils import summarize, predict_class, load_model_from_files, get_key_words

app = Flask(__name__)
api = Api(app)
tokenizer, model = load_model_from_files()


class TextProcessing(Resource):
    def post(self):
        data = request.get_json()
        text = data['text']
        
        summary = summarize(text)
        tags = get_key_words(text, summary)
        
        clas = predict_class(text, tokenizer, model)
        
        info = {'summary': summary, 'tags': tags, "meta_class": clas}
        json_data = json.dumps(info, ensure_ascii=False)  # Changed ensure_ascii to False
        
        return Response(json_data, content_type='application/json; charset=utf-8')
    
api.add_resource(TextProcessing, '/process_text')

if __name__ == '__main__':
    app.run(debug=True)