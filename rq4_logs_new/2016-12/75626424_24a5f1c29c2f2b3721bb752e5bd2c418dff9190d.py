from app import app


@app.route('/down', methods=['GET'])
def get_down():
    # do something
    pass

@app.route('/down', methods=['POST'])
def add_down():
    # do something
    pass

@app.route('/down', methods=['DELETE'])
def delete_down():
    # do something
    pass

@app.route('/down', methods=['PATCH'])
def update_down():
    # do something
    pass