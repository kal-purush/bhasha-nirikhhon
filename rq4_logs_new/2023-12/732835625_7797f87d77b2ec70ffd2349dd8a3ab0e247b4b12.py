import openai
from openai import OpenAI
from PyQt5.QtWidgets import QApplication

def gpt_for_txt_SingleProcess(model, api_key, message_input, progressBar):
    '''
    使用chatgpt实现对单张图像进行命名, 用于一次性调用
    注: 因为chagpt的回答方式是流处理，所用函数中涉及的进度显示是一种伪进度。
       具体的参数设置需要根据stream中的大致循环测试自己设定，不具备实际参考价值
    :param model:  所用的chatgpt模型的具体型号
    :param api_key:     API密钥
    :param message_input:    用户提问信息, 一般是待处理的图像提取内容
    :param progressBar:      处理进度控件
    :return:
    '''
    client = OpenAI(
    base_url="https://oneapi.xty.app/v1",
    api_key=api_key
    )

    stream = client.chat.completions.create(
        model=model,
        messages=[{"role":"system","content": "精炼下面的回答，要求在不改变语义的情况下精简句子"},
                  {"role": "user", "content": "请将下面这段描述缩写至10个token，并且以XX图的形式表达: 这张图中有一只小猫在吃食物，旁边趴着一只小兔子"},
                  {"role": "assistant", "content": "图: 小猫吃食物,小兔趴在旁边"},
            {"role": "user", "content": message_input}],
        stream=True,
    )
    process = 30
    progressBar.setValue(process)

    result = ''
    counter = 0
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
           result = result + chunk.choices[0].delta.content
        #更新处理进度
        counter = counter + 1
        if counter % 5 == 0 and process < 90:
            process = process + 5
            progressBar.setValue(process)

    if result == '':
        result = 'GPT模型提取失败, 请检查网络后重新尝试'
    
    return result


def gpt_for_txt_BatchProcess(model, api_key, message_input_list, textBrowser, progressBar):
    '''
    使用chatgpt实现对单张图像进行命名, 用于一次性调用
    注: 因为chagpt的回答方式是流处理，所用函数中涉及的进度显示是一种伪进度。
       具体的参数设置需要根据stream中的大致循环测试自己设定，不具备实际参考价值
    :param model:  所用的chatgpt模型的具体型号
    :param api_key:     API密钥
    :param message_input_list:    用户提问信息, 一般是待处理的图像提取内容
    :param textBrowser:      富文本框控件，用来时刻显示当前处理结果
    :param progressBar:      处理进度控件
    :return:
    '''
    client = OpenAI(
        base_url="https://oneapi.xty.app/v1",
        api_key=api_key
    )
    progressBar.setValue(10)

    num_content = len(message_input_list)  #当前待处理的文本数量
    naming_list = []     #用来存储所有的命名结果
    current_process = 10  # 当前处理进度
    step_process = int((95 - current_process) / num_content)  # 处理一个文本的进展幅度
    for i in range(0, num_content):
        parts = message_input_list[i].split(": ")
        message_input = parts[1]   #要": "后面的字符
        print(message_input)
        stream = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": "精炼下面的回答，要求在不改变语义的情况下精简句子，不要出现图字"},
                      {"role": "user",
                       "content": "请将下面这段描述缩写至5个token: 烟台红富士苹果，脆甜多汁，香甜可口"},
                      {"role": "assistant", "content": "烟台红富士苹果脆甜可口"},
                      {"role": "user", "content": "将下面的句子缩写至5个token :" + message_input}],
        stream=True
        )
        result = ''
        for chunk in stream:
            if chunk.choices[0].delta.content is not None:
                result = result + chunk.choices[0].delta.content

        if result == '':
            result = 'GPT模型提取失败, 请检查网络后重新尝试'
        else:
            result = '图' + str(i) + ': ' + result
            naming_list.append(result)
            #显示最新处理结果并更新进度条
            textBrowser.append(result)
            current_process = current_process + step_process
            progressBar.setValue(current_process)
            QApplication.processEvents()

    return naming_list

# imgs
# tf = ''
# model_name = "gpt-3.5-turbo"
# api_key = "sk-yIjuHjfVC2v4Of88050b6a4404444d9eAdD507DfC1Dd382d"
# message_input = "将下面的句子缩写至5个token :" + "这张图中有一只小猫在吃食物，旁边趴着一只小兔子"
# naming_txt = gpt_for_txt(model=model_name, api_key=api_key, message_input=message_input,progressBar=tf)
# print(naming_txt)