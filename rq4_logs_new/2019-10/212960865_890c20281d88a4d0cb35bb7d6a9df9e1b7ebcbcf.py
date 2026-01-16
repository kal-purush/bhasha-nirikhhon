'''
判断是否为顺子(顶墩，中墩，底墩)
'''
def ShunZi_Judge(pai,l):
    shunzi_5 = ['23456','34567','45678','56789','678910','78910J','8910JQ','910JQK','10JQKA']
    shunzi_3 = ['234','345','456','567','678','789','8910','910J','10JQ','JQK','QKA']
    if not l :
        return False
    if l == 5 or l == 6:   #5张顺子
        if pai in shunzi_5:
            return 5
        else:
            return 0
    if l == 3:   #3张顺子
        if pai in shunzi_3:
            return 3
        else :
            return 0

    if l == 8:
        for shunzi_5_i in shunzi_5:
            for shunzi_3_j in shunzi_3:
                s1 = shunzi_5_i + shunzi_3_j    #3+5顺子牌型
                s2 = shunzi_3_j + shunzi_5_i    #5+3顺子牌型
                if pai == s1 or pai == s2:
                    return 8
        return 0
    else:                          #5+5顺子牌型
        for i in range(4):
            if pai == shunzi_5[i] + shunzi_5[i + 5] or pai == shunzi_5[i + 5] + shunzi_5[i]:
                return 10
        return 0