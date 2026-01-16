from flask import Flask
import text_replies

api = Flask(__name__)
api.debug = True


@api.route('/dirk', methods=['GET'])
def get_dirk():
    return text_replies.dirk_reply()


@api.route('/erich', methods=['GET'])
def get_erich():
    return text_replies.erich_reply()


@api.route('/michi', methods=['GET'])
def get_michi():
    return text_replies.michi_reply()


@api.route('/ueli', methods=['GET'])
def get_ueli():
    return text_replies.ueli_reply()


@api.route('/sandro', methods=['GET'])
def get_sandro():
    return text_replies.sandro_reply()


if __name__ == '__main__':
    api.run(host='0.0.0.0')