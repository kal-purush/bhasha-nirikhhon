msgs = [
    "Enter an equation",
    "Do you even know what numbers are? Stay focused!",
    "Yes ... an interesting math operation. You've slept through all classes, haven't you?",
    "Yeah... division by zero. Smart move...",
    "Do you want to store the result? (y / n):",
        "Do you want to continue calculations? (y / n):",
    " ... lazy",
    " ... very lazy",
    " ... very, very lazy",
    "You are",
    "Are you sure? It is only one digit! (y / n)",
    "Don't be silly! It's just one number! Add to the memory? (y / n)",
    "Last chance! Do you really want to embarrass yourself? (y / n)",
    ]

memory = 0
def is_one_digit(v):
    if v > -10 and v < 10 and v.is_integer() == True: return True
    else: return False
def check(v1, v2, v3):
    msg = ""
    if is_one_digit(v1) == True and is_one_digit(v2) == True:
        msg = msg + msgs[6]
    if (v1 == 1 or v2 == 1) and v3 == '*':
        msg = msg + msgs[7]
    if (v1 == 0 or v2 == 0) and (v3 == '*' or v3 == '-' or v3 == '+'):
        msg = msg + msgs[8]
    if msg != "":
        msg = msgs[9] + msg
        print(msg)


while True:
    try:
        print(msgs[0])
        x, oper, y = input().split()
        if x == "M": x = memory
        if y == 'M': y = memory
        x = float(x)
        y = float(y)
        check(x, y, oper)
        if oper == "+": calc = x + y
        elif oper == '-': calc = x - y
        elif oper == '/': calc = x / y
        elif oper == '*': calc = x * y
        else:
            raise TypeError

    except ValueError:
        print(msgs[1])
    except ZeroDivisionError:
        print(msgs[3])
    except TypeError:
        print(msgs[2])
    else:
        print(calc)
        answer_2=''
        while True:
            if not answer_2 == 'n':
                print(msgs[4])
                answer = input()
            else: answer = 'n'
            if answer == 'y' or 'n':
                if answer== 'y':
                    if is_one_digit(calc) == True:
                        msg_index = 10
                        while True:
                            print(msgs[msg_index])
                            answer = input()
                            if answer == 'y':
                                if msg_index < 12:
                                    msg_index += 1
                                    continue
                                else:
                                    memory = calc
                                    break
                            elif answer == 'n': break
                            else: continue

                    else:
                        memory = calc
                print(msgs[5])
                answer_2 = input()
                if answer_2 == 'y' or 'n':
                    break
                else:
                    answer_2 = 'n'
                    continue
            else:continue
        if answer_2 == 'n': break
        else: continue

