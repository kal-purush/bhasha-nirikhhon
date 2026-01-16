from flask import Flask, Response,request
from docx import Document
import json
import os
import re
app = Flask(__name__)

@app.route('/', methods=['POST'])
def posttest():
   return 'hi',200

@app.route('/uploadDocx/', methods=['POST']) # 上传路径
def uploadDocx():
    form = request.form #获得表单数据 除了file
    data = json.loads(json.dumps(form)) #转成dict
    titles = pickTitles(data) #需要匹配的title
    print(titles)

    #获取文件
    files = request.files.getlist('files[]')
    print(files)

    fileSavePath = []
    for file in files:
        filename = os.path.join('../RECEIVE/',file.filename)
        fileSavePath.append(filename)
        file.save(filename)

    # 提取文档内容并分类
    pickOutText = dealWithDocx(fileSavePath[0], titles)
    return {'para': pickOutText}, 200

    # # test
    # file = request.files['file']
    # filename = os.path.join('../RECEIVE/',file.filename)
    # file.save(filename)

    # pickOutText = dealWithDocx(filename, titles)

    # print(pickOutText)
    # return 'test', 200
    

#返回需要匹配的title
def pickTitles(data):
    titles = []
    for key in data:
        if re.search(r'title(\d+)$',key) != None:
            titles.append(data[key])
    return titles


def dealWithDocx(file, titles):
    result = []

    temp_title = ''
    temp_content = []

    #获取文档
    raw = Document(file)
    # for part in raw.part._rels:
    #     print(part)

    for para in raw.paragraphs:
        line = para.text
        # print(line)
        is_match = False
        for title in titles:
            # 编码后匹配
            pattern_encode = title.encode('utf-8').strip()
            line_encode = line.encode('utf-8').strip()

            if re.match(pattern_encode, line_encode):
                if temp_title != '':
                    result.append({
                        temp_title : ''.join(temp_content)
                    })
                    temp_title = title
                    temp_content = []
                else:
                    temp_title = title
                    temp_content = []
                is_match = True
                break
        if not is_match:
            if temp_title != '':
                temp_content.append(line)
    # print(result)
    return result

if __name__ == '__main__':
    app.run()