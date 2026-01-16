#!/urs/bin/env python
# -*- coding:utf-8 -*-

"""

模拟计算器开发：
实现加减乘除及拓号优先级解析
用户输入 1 - 2 * ( (60-30 +(-40/5) * (9-2*5/3 + 7 /3*99/4*2998 +10 * 568/14 )) - (-4*3)/ (16-3*2) )等类似公式后，
必须自己解析里面的(),+,-,*,/符号和公式(不能调用eval等类似功能偷懒实现)，运算后得出结果，
结果必须与真实的计算器所得出的结果一致

"""

import re
import sys


#提取最里面的表达式
def exec_brackets(expression):
    pattern = re.compile(r"\([^()]+\)")
    if "(" in expression:
        expression = re.search(pattern, expression).group()
    return expression


#计算乘除
def compute_mul_div(expression):
    pattern1 = re.compile(r"[\*\/]")
    pattern2 = re.compile(r"[\+\-\*\/]|\d+\.?\d*")

    while "*" in expression or "/" in expression:
        operator = pattern1.search(expression).group()
        data_list = re.findall(pattern2, expression)

        if operator == "*":
            pos = data_list.index("*")
            if "-" not in data_list[pos+1]:
                n1, n2 = data_list[pos-1], data_list[pos+1]
                obj = float(n1) * float(n2)
                expression = expression.replace("".join(data_list[pos-1:pos+2]), str(obj))
            else:
                n1, n2 = data_list[pos-1], data_list[pos+2]
                obj = -(float(n1) * float(n2))
                expression = expression.replace("".join(data_list[pos-1:pos+3]), str(obj))
        else:
            pos = data_list.index("/")
            if "-" not in data_list[pos+1]:
                n1, n2 = data_list[pos-1], data_list[pos+1]
                obj = float(n1) / float(n2)
                expression = expression.replace("".join(data_list[pos-1:pos+2]), str(obj))
            else:
                n1, n2 = data_list[pos-1], data_list[pos+2]
                obj = -(float(n1) / float(n2))
                expression = expression.replace("".join(data_list[pos-1:pos+3]), str(obj))

    return expression

#计算加减
def compute_add_sub(expression):

    while "+-" in expression or "-+" in expression or "++" in expression or "--" in expression:
        expression = expression.replace("+-", "-")
        expression = expression.replace("-+", "-")
        expression = expression.replace("++", "+")
        expression = expression.replace("--", "+")

    pattern1 = re.compile(r"[\+\-]")
    pattern2 = re.compile(r"[\+\-]|\d+\.?\d*")
    data_list = re.findall(pattern2, expression)

    while len(data_list) > 2:
        if data_list[0] == '-':
            operator = re.search(pattern1, expression[1:]).group()
        else:
            operator = pattern1.search(expression).group()

        if operator == '+':
            pos = data_list.index('+')
            n1, n2 = data_list[pos-1], data_list[pos+1]
            if data_list[0] == '-':
                obj = float(n2) - float(n1)
            else:
                obj = float(n1) + float(n2)
            expression = expression.replace("".join(data_list[:pos+2]), str(obj))
        else:
            if data_list[0] == '-':
                pos = data_list[1:].index('-')
                pos += 1
                n1, n2 = data_list[pos-1], data_list[pos+1]
                obj = -(float(n1) + float(n2))
            else:
                pos = data_list.index('-')
                n1, n2 = data_list[pos-1], data_list[pos+1]
                obj = float(n1) - float(n2)
            expression = expression.replace("".join(data_list[:pos+2]), str(obj))
        data_list = re.findall(pattern2, expression)

    return expression


def main():

    print(
        """
        --------------------------------
            欢迎使用python计算器
        --------------------------------
        注：'q'为退出计算器
        """)

    while True:

        user_input = input("Enter expression : ")
        user_input = re.sub("\s+", "", user_input)

        if len(user_input) == 0:
            continue
        elif user_input == 'q':
            sys.exit()
        elif user_input.count("(") != user_input.count(")"):
            print("\t\033[0;31m输入表达式有误，注意括号配对！\033[0m")
            continue
        elif re.search("[^0-9\.\+\-\*\/\(\)]", user_input):
            print("\t\033[0;31m输入表达式有误，注意存在无效字符！\033[0m")
            continue

        while "(" in user_input:
            deep_expr = exec_brackets(user_input)
            mul_div_expr = deep_expr[1:-1]
            add_sub_expr = compute_mul_div(mul_div_expr)
            obj = str(compute_add_sub(add_sub_expr))
            user_input = user_input.replace(deep_expr, obj)
        else:
            add_sub_expr = compute_mul_div(user_input)
            obj = str(compute_add_sub(add_sub_expr))
            print("\t\033[0;32m表达式计算结果为：%s\033[0m" % obj)


if __name__ == '__main__':
    main()