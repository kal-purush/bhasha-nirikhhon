#coding=utf8
#!/etc/bin/python


import re
import sys
import json

def analyze_log(target_mobile, log_file=None):
    # Todo: try to refactor the log collect from database or rewrite the
    # implementation with GoLang 

    res = [] 
    mobile_pattern = re.compile(target_mobile)
    err_pattern = re.compile('err_code')
    with open(log_file, 'r') as f:
        for each in f.readlines():
           
            if re.search(mobile_pattern, each) and re.search(err_pattern, each):
                res.append(each)
                print each

    return res 

def get_all_index(patter, s):

    return [each.start() for each in re.finditer(patter, s)]

def extract_error_code(err_logs):
    err_code = []
    err_msg = []
    for each_line in err_logs:
        heads = get_all_index('{', each_line)
        tails = get_all_index('}', each_line)

        for i in range(len(heads)):
            s = each_line[heads[i]: tails[i]+1]
            d = json.loads(s)
            each_err_msg = d.get('err_msg', None)
            each_err_code = d.get('err_code', None)
            if each_err_code:
                err_code.append(each_err_code)
            if each_err_msg:
                err_msg.append(each_err_msg)

    return set(err_code), set(err_msg)


if __name__ == '__main__':
    print sys.argv
    if len(sys.argv) == 2:
        mobile = sys.argv[1]
    else:
        mobile = None


    logs =  analyze_log(mobile)

    print extract_error_code(logs)