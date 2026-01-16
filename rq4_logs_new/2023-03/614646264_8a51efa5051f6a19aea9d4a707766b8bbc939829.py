from flask import Flask, render_template, request, jsonify
from handlingKeyword import find_some_keywords
import requests, random, json
from secrets import API_KEY

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

def pickRandom(n):
    return random.randint(0,n-1)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process-data', methods=['POST'])
def process_data():
    data = request.get_json()  # get the JSON data from the request
    # process the data here
    res = find_some_keywords(data['content'])
    final_res = []
    for (score, word) in res:
        if(score > 10):
            final_res.append((score, word))
    return jsonify(final_res)

@app.route('/handleAPIcalls', methods=['POST'])
def handlingAPICall():
    categories = ['business', 'entertainment', 'health', 'politics', 'top','science','environment'];
    countries = ['us', 'ca', 'au', 'gb','nz'];
    category = categories[pickRandom(len(categories))]
    country = countries[pickRandom(len(countries))]
    url = "https://newsdata.io/api/1/news?apikey="+API_KEY+"&category="+category+"&country="+country+""
    res = requests.get(url)
    data = res.text
    parse_json = json.loads(data)
    results = parse_json['results']
    # print(data)
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)