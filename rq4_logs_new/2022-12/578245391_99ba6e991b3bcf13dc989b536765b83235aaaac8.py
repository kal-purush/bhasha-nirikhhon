import random, os

os.chdir(os.path.dirname(__file__))


def something_input(in_type, wellcome_text):
    input_str = input(f'{wellcome_text} : ')
    if in_type == 'int':
        result = int(input_str)
    elif in_type == 'float':
        result = float(input_str)
    else:
        result = input_str
    return result


def split_string(string):
    string = string.replace('+', ' + ')
    string = string.replace('-', ' - ')
    string = string.replace('*', ' * ')
    string = string.replace('/', ' / ')
    return string.split()

def calc_parse(digit_lst):
    new_list = []
    for i in range(len(digit_lst)):
        if digit_lst[i].isdigit() and i > 0:
            digit_lst[i] = float(digit_lst[i])
            if digit_lst[i-1] == '+':
                new_list.append(digit_lst[i])
            elif digit_lst[i-1] == '-':
                new_list.append(digit_lst[i] * -1)
            elif digit_lst[i-1] == '*':
                new_list[-1] = digit_lst[i] * new_list[-1]
            elif digit_lst[i-1] == '/':
                new_list[-1] = new_list[-1] / digit_lst[i]
        elif digit_lst[i].isdigit() and i == 0:
            digit_lst[i] = float(digit_lst[i])
            new_list.append(digit_lst[i])
    return sum(new_list)


string2 = '(7-8+3)*5+1+(1+23)*3'

# '(7-8+3)'*5+1+(1+23)*3'
print(string2)

for i in range(len(string2)):
    string3 = ''
    if string2[i] == '(':
        j = i + 1
        str_tmp = ''
        while string2[j] != ')':
            str_tmp += string2[j]
            j += 1
        tmp_float = calc_parse(split_string(str_tmp))
        string3 += str(tmp_float)
print(string3)

quit()
string = '7-8+3*5+1+1+23*3'
digit_lst = split_string(string)
print(calc_parse(digit_lst))