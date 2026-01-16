import json
import os

from flask import Flask, jsonify, request

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True  # 实时更新模板文件
app.config['MAX_CONTENT_LENGTH'] = 1000 * 1024 * 1024  # 上传文件最大限制1000 MB
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # 静态文件缓存时间，默认值为 12 小时。可以通过将其设为 0 来禁止浏览器缓存静态文件
app.config['JSONIFY_TIMEOUT'] = 6000  # 设置响应超时时间为 6000 秒
app.config['PROXY_CONNECT_TIMEOUT'] = 6000
app.config['PROXY_SEND_TIMEOUT'] = 6000
app.config['PROXY_READ_TIMEOUT'] = 6000

# 存放数据库文件的路径
DB_PATH = "/app/db"

message_dict = {}


@app.route('/api/data', methods=['GET'])
def get_data():
    global message_dict
    returndict = jsonify(message_dict)
    message_dict.clear()
    return returndict


@app.route('/api/ping', methods=['GET'])
def ping():
    return jsonify({'result': ''})


@app.route('/api/data', methods=['POST'])
def post_data():
    global message_dict
    message = request.form.get('message')
    message_dict[message] = ''
    return jsonify({'result': ''})


@app.route('/api/data2', methods=['POST'])
def post_data_key_data():
    cachekey = request.form.get('cacheKey')
    action = request.form.get('action')
    file_path = os.path.join(DB_PATH, f'{cachekey}.txt')
    if action == 'get_single':
        try:
            with open(file_path, "rb") as f:
                data_bytes = f.read()
            return jsonify({'result': data_bytes.decode('utf-8')})
        except Exception as e:
            return jsonify({'result': ''})
    # 删除一个数据表
    elif action == 'delete':
        try:
            os.remove(file_path)
            return jsonify({'result': 1})
        except Exception as e:
            return jsonify({'result': 0})
    # map增加表中一些条数据
    elif action == 'add_map':
        try:
            # 字符串格式的json数据
            add_dict = json.loads(request.form.get('dict_data'))
            if len(add_dict) == 0:
                return jsonify({'result': 0})
            old_dict = {}
            try:
                with open(file_path, "rb") as f:
                    data_bytes = f.read()
                old_dict = json.loads(data_bytes.decode('utf-8'))
            except Exception as e:
                # 如果文件不存在，创建一个新文件
                open(file_path, "wb").close()
            old_dict.update(add_dict)
            # 清除文件中的所有数据
            with open(file_path, "wb") as f:
                pass
            # 复写数据
            with open(file_path, "wb") as f:
                f.write(json.dumps(old_dict).encode('utf-8'))
            return jsonify({'result': 1})
        except Exception as e:
            return jsonify({'result': 0})
    # 增加/修改表中一条数据
    elif action == 'add_single':
        try:
            # 字符串格式的json数据
            value = request.form.get('dict_data')
            try:
                # 如果文件存在，清除文件中的所有数据
                with open(file_path, "wb") as f:
                    pass
            except:
                # 如果文件不存在，创建一个新文件
                open(file_path, "wb").close()
            # 复写数据
            with open(file_path, "wb") as f:
                f.write(value.encode('utf-8'))
            return jsonify({'result': 1})
        except Exception as e:
            return jsonify({'result': 0})
    # 删除map里的单个key/多个key
    elif action == 'delete_keys':
        try:
            # 字符串格式的json数据
            dict_data = json.loads(request.form.get('dict_data'))
            with open(file_path, "rb") as f:
                data_bytes = f.read()
            dict = json.loads(data_bytes.decode('utf-8'))
            new_dict = {}
            for key, value in dict.items():
                if key in dict_data:
                    continue
                new_dict[key] = value
            # 清除文件中的所有数据
            with open(file_path, "wb") as f:
                pass
            # 复写数据
            with open(file_path, "wb") as f:
                f.write(json.dumps(new_dict).encode('utf-8'))
            return jsonify({'result': 1})
        except Exception as e:
            return jsonify({'result': 0})
    # 查询map
    elif action == 'get_map':
        try:
            with open(file_path, "rb") as f:
                data_bytes = f.read()
            return jsonify(json.loads(data_bytes.decode('utf-8')))
        except Exception as e:
            return jsonify({})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=22772)