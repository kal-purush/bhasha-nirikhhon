from flask import Flask, jsonify, request, json, abort

app = Flask(__name__)

# Define a list of documents with nested sections
documents = [
    {
        "name": "root_document",
        "text": "Some text here",
        "sections": [
            {
                "name": "subtitle_1",
                "sections": [
                    {
                        "name": "title_1",
                        "text": "Text for title 1",
                        "sections": []
                    }        
                ]
            },
            {
                "name": "subtitle_2",
                "text": "Text for Subtitle 2",
                "sections": []
            }
        ]
    }
]

# Define a route to get the section of a document
@app.route('/document', methods=['GET'])
def get_dictionary():
    query = request.args.get('section')
    response = getDocumentSection(query=query)
    if not response:
        abort(404, description=f'Invalid path: "%s"' % query)
    return jsonify(response)

# Define a route to add a section to a document
@app.route('/document', methods=['POST'])
def add_to_dictionary():
    data = json.loads(request.get_data())
    if not data.get('path'):
        abort(404, description='Path value is required')
    res = addContent(data)
    if not res[0]:
        abort(404, description=res[1])
    return jsonify('OK')

# Get a section of a document by its name
def getDocumentSection(query):
    route = query.split('.')
    response = getContentByName(documents, route[0])
    if not response:
        return None
    for name in route[1:]:
        response = getContentByName(
            response.get('sections', []), name)
    return response

# Add a new section to a document
def addContent(data):
    route = data.get('path', '').split('.')
    root_exists = getContentByName(documents, route[0])
    if len(route) == 1 and not root_exists:
        return addDocument(data)
    else:
        return addSection(data)

# Add a new document
def addDocument(data):
    documents.append({
        'name': data.get('path', ''),
        'sections': [{
            'name': data.get('name', ''),
            'text': data.get('text', ''),
        }]
    })
    return True, ''

# Add a section to a document
def addSection(data):
    route = data.get('path', '').split('.')
    target_list = documents
    for name in route:
        name_found = False
        for i in range(len(target_list)):
            if name == target_list[i].get('name'):
                name_found = True
                target_list = target_list[i]
                if 'sections' not in target_list:
                    target_list['sections'] = []
                target_list = target_list['sections']
                break
        if not name_found:
            return False, f'Invalid path: "%s"' % data.get('path') 
    target_list.append({
        'name': data.get('name', ''),
        'text': data.get('text', ''),
    })
    return True, ''

# Get a content from a list of contents by its name
def getContentByName(my_list, name):
    return next(filter(lambda x: x['name'] == name, my_list), None)

if __name__ == '__main__':
    app.run(debug=True)