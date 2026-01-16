CHAR_ARR =  ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v',
             'w','x','y','z','1','2','3','4','5','6','7','8','9','0',]
MEMR_ARR = []
SYS_KEYS = [0, 0, None]
OP_ARR = [">", "<", "+", "-", "*", "/"]
LOCAL_ARR = ["CHAR", "MEMR"]
FUNC_ARR = ["MOVE", "PULL", "CRPS", "GET", "MSIZE", "MSET", "PUSH", 'MATH', "MTRW"]
COUNTER = 0

def getcmd(SYS_KEYS, COUNTER):
    SYS_KEYS[0] = 0
    SYS_KEYS[1] = 0
    SYS_KEYS[2] = None
    COUNTER = 0
    document = input("Input filename: ")
    main(SYS_KEYS, COUNTER, document)
    pass

def main(SYS_KEYS, COUNTER, file):
    script_file = open(file, "r")
    content = script_file.readlines()
    #print(len(content))
    for obj in content:
        #print("Here!")
        #print("Content: " + content[COUNTER])
        content_line = content[COUNTER].split(';')
        process(content_line[obj.index(obj)], obj.index(obj), SYS_KEYS)
        #print("Content line: " + content_line[obj.index(obj)])
        COUNTER += 1
    script_file.close()
    print(script_file.closed)
    getcmd(SYS_KEYS, COUNTER)
    pass

def process(content_line, line, SYS_KEYS):
    token_list = {}
    words = content_line.split(" ")
    #print("Words of line num. " + str(line + 1) + " are: ")
    for obj in words:
        if obj in FUNC_ARR:
            token_list["func"] = obj
        else:
            if obj in OP_ARR:
                token_list["op"] = obj
            else:
                if obj in LOCAL_ARR:
                    token_list["list"] = obj
                else:
                    try:
                        if token_list["arg1"]:
                            token_list["arg2"] = obj
                    except KeyError:
                        token_list["arg1"] = obj
    #print(token_list)
    '''
    if exec(token_list, SYS_KEYS, SYS_KEYS[2]) != 0:
        print("[ERROR]")
    else:
        pass
    pass
    '''
    exec(token_list, SYS_KEYS)
def exec(token_list, SYS_KEYS):
    try:
        if token_list["func"] == FUNC_ARR[0]:
            MOVE(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[1]:
            PULL(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[2]:
            CRPS(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[3]:
            GET(token_list, SYS_KEYS)
        if token_list['func'] == FUNC_ARR[4]:
            MSIZE(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[5]:
            MSET(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[6]:
            PUSH(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[7]:
            SYS_KEYS[2] = MATH(token_list, SYS_KEYS)
        if token_list["func"] == FUNC_ARR[8]:
            MTRW(SYS_KEYS)
        return 0
    except Exception:
        return 1
    pass

def MOVE(token_list, SYS_KEYS):
    if token_list["list"] == LOCAL_ARR[0]:
        if token_list["op"] == OP_ARR[0] or OP_ARR[1]:
            if token_list["op"] == "<":
                SYS_KEYS[0] -= int(token_list["arg1"])
            if token_list["op"] == ">":
                SYS_KEYS[0] += int(token_list["arg1"])

    if token_list["list"] == LOCAL_ARR[1]:
        if token_list["op"] == OP_ARR[0] or OP_ARR[1]:
            if token_list["op"] == "<":
                SYS_KEYS[1] -= int(token_list["arg1"])
            if token_list["op"] == ">":
                SYS_KEYS[1] += int(token_list["arg1"])
    pass

def PULL(token_list, SYS_KEYS):
    if token_list["list"] == LOCAL_ARR[0]:
        print(CHAR_ARR[SYS_KEYS[0]])
    if token_list["list"] == LOCAL_ARR[1]:
        print(MEMR_ARR[SYS_KEYS[1]])
    pass

def CRPS(token_list, SYS_KEYS):
    if token_list["list"] == LOCAL_ARR[0]:
        print(SYS_KEYS[0])
    if token_list["list"] == LOCAL_ARR[1]:
        print(SYS_KEYS[1])
    pass

def GET(token_list, SYS_KEYS):
    MEMR_ARR[SYS_KEYS[1]] = input()
    pass

def MSET(token_list, SYS_KEYS):
    SET_COUNT = 0
    while SET_COUNT != int(token_list["arg1"]):
        MEMR_ARR.append(None)
        SET_COUNT += 1
    pass

def MSIZE(token_list, SYS_KEYS):
    print(len(MEMR_ARR))
    pass

def MATH(token_list, SYS_KEYS):
    if token_list["op"] in OP_ARR:
        if token_list["op"] == "-":
            res = OP_SUB(token_list["arg1"], token_list["arg2"])
            return res
        if token_list["op"] == "+":
            res = OP_SUM(token_list["arg1"], token_list["arg2"])
            return res
    pass

def OP_SUM(arg1, arg2):
    res = int(arg1) + int(arg2)
    return res
    pass

def OP_SUB(arg1, arg2):
    res = int(arg1) - int(arg2)
    return res
    pass

def PUSH(token_list, SYS_KEYS):
    if MEMR_ARR[SYS_KEYS[1]] != None:
        return 1
    else:
        MEMR_ARR[SYS_KEYS[1]] = SYS_KEYS[2]
    pass

def MTRW(SYS_KEYS):
    MEMR_ARR[SYS_KEYS[1]] = None
    print(MEMR_ARR)
    pass

def LOAD(token_list, SYS_KEYS):
    if token_list["list"] == LOCAL_ARR[0]:
        SYS_KEYS[2] = CHAR_ARR[SYS_KEYS[0]]
        return SYS_KEYS[2]
    pass

if __name__ == '__main__':
    getcmd(SYS_KEYS, COUNTER)