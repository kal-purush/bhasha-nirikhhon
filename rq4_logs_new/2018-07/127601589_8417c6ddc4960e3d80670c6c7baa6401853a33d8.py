'''
由大小写字母组合，生成激活码。

输入激活码长度lenght和数量num。

例：
  
长度为10，数量为5
>>>generate_code(10, 5)
>>>['BOOLQJJUrj', 'cBHKHlxCkt', 'MfCznkpsiC', 'FiqsRyyZvt', 'wWdFagmJYJ']
'''


from random import choice


def generate_code(length, num):
    activation_codes = []
    ascii_letter = [chr(i) for i in range(65, 91)] + [chr(i) for i in range(97, 123)]
    for i in range(num):
        activation_code = ''
        for j in range(length):
            activation_code += choice(ascii_letter)
        activation_codes.append(activation_code)
    return activation_codes