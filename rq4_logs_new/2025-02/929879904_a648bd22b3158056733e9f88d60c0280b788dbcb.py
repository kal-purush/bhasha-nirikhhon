from script.chatgpt import generate_response_langchat
from script.repo import clone_github_repo
from quart import Quart, request
from quart_cors import cors

app = Quart(__name__)
app = cors(app, allow_origin="*", allow_headers="*")


@app.route('/lang-chat-sources', methods=['POST'])
async def langchat_sources():
    req_data = await request.get_json()
    api_key = request.headers.get('Authorization').split(" ")[1]
    user_input = req_data['user_input']
    url = req_data['url']

    response, sources = await generate_response_langchat(user_input, api_key, url)

    resp_data = {"response": response,
                 "sources": [doc.__dict__ for doc in sources]}
    return resp_data, 200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': '*'}


@app.route('/extract', methods=['POST'])
async def extract_repo():
    req_data = await request.get_json()
    api_key = request.headers.get('Authorization').split(" ")[1]
    url = req_data['url']

    response = await clone_github_repo(url)
    resp_data = {"response": response}
    return resp_data, 200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': '*'}


if __name__ == '__main__':
    app.run()