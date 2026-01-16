import re

CONST_ALPHABET = ['a','b','c','*']

# str rule
# return lst
def ruleStrToList(rule):
    flag = True
    res = re.match('(.*)\\|->'+'(.*)$', rule, re.MULTILINE)
    if (isinstance(res, type(None))):
        flag = False
        res = re.match('(.*)->'+'(.*)$', rule, re.MULTILINE)
    if (isinstance(res, type(None))):
        return []
    return [res.group(1), res.group(2), flag]


# lst rule
# return lst
def validateRuleList(rule):
    i = 0
    for k in rule[0]:
        if k not in CONST_ALPHABET:
            return [i,k]
        i += 1
    if rule[2]:
        i += 3
    else:
        i += 2
    for k in rule[1]:
        if k not in CONST_ALPHABET:
            return [i,k]
        i += 1
    return []