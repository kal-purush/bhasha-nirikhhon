from sort_cards import *
from shunzi_judge import  *
from httpfunctions import *
from copy import *
#用1-14依次标识特殊牌型

#至尊青龙
def ZhiZunQingLONG(cards):
    pai = ""
    for card in cards:
        pai += card
    zhizunqingLong = ["&2&3&4&5&6&7&8&9&10&J&Q&K&A", "#2#3#4#5#6#7#8#9#10#J#Q#K#A",
                      "*2*3*4*5*6*7*8*9*10*J*Q*K*A", "$2$3$4$5$6$7$8$9$10$J$Q$K$A"]
    if pai in zhizunqingLong:
        return True
    return False

#一条龙
def YiTiaoLong(cards):
    paishu = {'2':1,'3':1,'4':1,'5':1,'6':1,'7':1,'8':1,'9':1,'10':1,'J':1,'Q':1,'K':1,'A':1}
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        paishu[lst_i] -= 1
        if paishu[lst_i] < 0:
            return False
    return True

#十二皇族
def ShiErHuangZu(cards):
    count = 0
    i = 0
    shierhuangzu = ['J','Q','K','A']
    for lst_i in cards:
        i += 1
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        if lst_i in shierhuangzu:
            count += 1
            if count >= 12:
                return True
        if i - count >= 2:
            return False
    return False

#三同花顺

tonghuashun_of_3cards_count = 0     #3张顺子牌型个数
tonghuashun_of_5cards_count = 0     #5张顺子牌型个数
def num_count(num):
    if num == 0:
        return
    global tonghuashun_of_5cards_count
    global tonghuashun_of_3cards_count
    if num:
        if num == 3:
            tonghuashun_of_3cards_count += 1

        elif num == 5:
            tonghuashun_of_5cards_count += 1

        elif num == 8:
            tonghuashun_of_5cards_count += 1
            tonghuashun_of_3cards_count += 1

        elif num == 10:
            tonghuashun_of_5cards_count += 2
def SanTongHuaShun(cards):
    global tonghuashun_of_3cards_count
    global tonghuashun_of_5cards_count
    tonghua = {'*':"",'#':"",'$':"",'&':""}
    len_huase = {'*':0,'#':0,'$':0,'&':0}
    for pai_i in cards:
        tonghua[pai_i[:1]] += pai_i[1:]
        len_huase[pai_i[:1]] += 1

    #四种花色组成的顺子（如果存在）牌型
    meihua = tonghua['*']
    heitao = tonghua['$']
    hongtao = tonghua['&']
    fangkuai = tonghua['#']

    #梅花同花顺个数
    num = ShunZi_Judge(meihua,len_huase['*'])
    num_count(num)

    #黑桃同花顺个数
    heitaotonghuashun_num = ShunZi_Judge(heitao,len_huase['$'])
    num_count(heitaotonghuashun_num)

    #红桃同花顺个数
    hongtaotonghuashun_num = ShunZi_Judge(hongtao,len_huase['&'])
    num_count(hongtaotonghuashun_num)

    #方块同花顺
    fangkuaitonghuashun_num = ShunZi_Judge(fangkuai,len_huase['#'])
    num_count(fangkuaitonghuashun_num)

    #判断是否为3同花顺即3+5+5牌型
    if tonghuashun_of_5cards_count == 2 and tonghuashun_of_3cards_count == 1:
        return True
    return False

#三分天下
def SanFenTianXia(cards):
    boom = {'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'J':0,'Q':0,'K':0,'A':0}
    boom_num = 0
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('&')
        lst_i = lst_i.strip('$')
        boom[lst_i] += 1
        if boom[lst_i] == 4:
            boom_num += 1
    if boom_num == 3:
        return True
    return False

#全大
def All_Big(cards):
    all_big = ['8','9','10','J','Q','K','A']
    for lst_i in cards:
        lst_i = lst_i.strip('&')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('*')

        if lst_i not in all_big:
            return False
    return True

#全小
def All_Small(cards):
    all_small = ['2','3','4','5','6','7','8']
    for lst_i in cards:
        lst_i = lst_i.strip('&')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('*')

        if lst_i not in all_small:
            return False
    return True

#凑一色
def CouYiSe(cards):
    meihua_and_fangkuai_num = 0
    hongtao_and_heitao_num = 0

    for lst_i in cards:
        if lst_i.find('*')!=-1 or lst_i.find('#')!=-1:
            meihua_and_fangkuai_num += 1
            if hongtao_and_heitao_num != 0:
                return False
        else :
            hongtao_and_heitao_num += 1
            if meihua_and_fangkuai_num != 0:
                return False
    return True

#双怪冲三
def ShuangGuaiChongSan(cards):
    duizi_num = 0
    santiao_num = 0
    card_dic = {'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'J':0,'Q':0,'K':0,'A':0}
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        card_dic[lst_i] += 1
        if card_dic[lst_i] == 2:
            duizi_num += 1
        if card_dic[lst_i] == 3:
            santiao_num += 1
            duizi_num -= 1
    if duizi_num == 3 and santiao_num == 2:
        return True
    return False

#四套三条
def SiTaoSanTiao(cards):
    santiao_num = 0
    card_dic = {'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'J':0,'Q':0,'K':0,'A':0}
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        card_dic[lst_i] += 1
        if card_dic[lst_i] == 3:
            santiao_num += 1
        if card_dic[lst_i] > 3:
            return False
    if santiao_num == 4:
        return True
    return False

#五对三条
def WuDuiSanTiao(cards):
    duizi_num = 0
    santiao_num = 0
    card_dic = {'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'J':0,'Q':0,'K':0,'A':0}
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        card_dic[lst_i] += 1
        if card_dic[lst_i] == 2:
            duizi_num += 1
        if card_dic[lst_i] == 3:
            santiao_num += 1
            if santiao_num > 1:
                return False
            duizi_num -= 1
    if duizi_num == 5 and santiao_num == 1:
        return True
    return False

#六对半
def LiuDuiBan(cards):
    duizi_num = 0
    card_dic = {'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'J':0,'Q':0,'K':0,'A':0}
    for lst_i in cards:
        lst_i = lst_i.strip('*')
        lst_i = lst_i.strip('#')
        lst_i = lst_i.strip('$')
        lst_i = lst_i.strip('&')
        card_dic[lst_i] += 1
        if card_dic[lst_i] == 2:
            duizi_num += 1
        if card_dic[lst_i] > 2:
            return False
    if duizi_num == 6:
        return True
    return False

#三顺子
def SanShunZi(cards):
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    card_list = ['2','3','4','5','6','7','8','9','10','J','Q','K','A']
    #对手牌进行排列，凑出顺子阵容
    for lst_i in cards:
        # l = lst_i.strip('*')
        # l = lst_i.strip('#')
        # l = lst_i.strip('$')
        # l = lst_i.strip('&')
        card_dic[lst_i[1:]] += 1

    new_pai = []
    count = 13
    while(count):
        pai = ""
        for i in range(13):
            if card_dic[card_list[i]] != 0:
                pai += card_list[i]
                card_dic[card_list[i]] -= 1
                count -= 1
        if pai != "":
            new_pai.append(pai)

    new_shoupai = new_pai[0]
    for i in range(1,len(new_pai)):
        l = len(new_pai[i])
        f = new_shoupai.find(new_pai[i])
        if f != -1:
            new_shoupai = new_shoupai[:f + l] + new_pai[i] + new_shoupai[f + l:]

    ###判断是否为三顺子
    shunzi_5 = ['23456', '34567', '45678', '56789', '678910', '78910J', '8910JQ', '910JQK', '10JQKA']
    shunzi_3 = ['234', '345', '456', '567', '678', '789', '8910', '910J', '10JQ', 'JQK', 'QKA']
    shun3 = ""
    shun5 = ""
    for shunzi3 in shunzi_3:
        new_shoupai_copy = new_shoupai
        shunzi_3_num = 0
        shunzi_5_num = 0
        if new_shoupai_copy.find(shunzi3) != -1:
            shunzi_3_num += 1
            #shun3 = shunzi3
            new_shoupai_copy2 = new_shoupai_copy.replace(shunzi3,"")
            for shunzi5 in shunzi_5:
                if new_shoupai_copy2.find(shunzi5) != -1:
                    new_shoupai_copy2 = new_shoupai_copy2.replace(shunzi5,"")
                    shunzi_5_num += 1
                    #shun5 += shunzi5
                    if shunzi_3_num == 1 and shunzi_5_num == 2:
                        return True
    return False

#三同花
def SanTongHua(cards):
    meihua_num = 0
    heitao_num = 0
    hongtao_num = 0
    fangkuai_num = 0
    for lst_i in cards:
        if lst_i.find('*')!=-1:
            meihua_num += 1
            if meihua_num > 5:
                return False
        elif lst_i.find('$')!=-1:
            heitao_num += 1
            if heitao_num > 5:
                return False
        elif lst_i.find('&')!=-1:
            hongtao_num += 1
            if heitao_num > 5:
                return False
        else:
            fangkuai_num += 1
            if fangkuai_num > 5:
                return False

    if meihua_num==3 and heitao_num==5 and hongtao_num==5 :
        return True
    if meihua_num==5 and heitao_num==3 and hongtao_num==5 :
        return True
    if meihua_num==5 and heitao_num==5 and hongtao_num==3 :
        return True
    if meihua_num==3 and heitao_num==5 and fangkuai_num==5 :
        return True
    if meihua_num==5 and heitao_num==3 and fangkuai_num==5 :
        return True
    if meihua_num==5 and heitao_num==5 and fangkuai_num==3 :
        return True
    if meihua_num==3 and hongtao_num==5 and fangkuai_num==5 :
        return True
    if meihua_num==5 and hongtao_num==3 and fangkuai_num==5 :
        return True
    if meihua_num==5 and hongtao_num==5 and fangkuai_num==3 :
        return True
    if heitao_num==3 and hongtao_num==5 and fangkuai_num==5 :
        return True
    if heitao_num==5 and hongtao_num==3 and fangkuai_num==5 :
        return True
    if heitao_num==5 and hongtao_num==5 and fangkuai_num==3 :
        return True
    return False

#手牌转字符串
def cards2string(cards):
    shangdun = cards[0] + ' ' + cards[1] + ' ' + cards[2]
    zhongdun = cards[3] + ' ' + cards[4] + ' ' + cards[5] + ' ' + cards[6] + ' ' + cards[7]
    xiadun = cards[8] + ' ' + cards[9] + ' ' + cards[10] + ' ' + cards[11] + ' ' + cards[12]
    return [shangdun, zhongdun, xiadun]

def special_cards_type(cards):
    Cards = sort_Cards(cards)
    best_cards = ""
    if ZhiZunQingLONG(Cards):
        print('1')
        return [True,cards2string(Cards)]
    elif YiTiaoLong(Cards):
        print('2')
        return [True,cards2string(Cards)]
    elif ShiErHuangZu(Cards):
        print('3')
        return [True,cards2string(Cards)]
    elif SanTongHuaShun(Cards):         ###考虑是否要特殊处理
        print('4')
        return [True,cards2string(Cards)]
    elif SanFenTianXia(Cards):
        print('5')
        return [True,cards2string(Cards)]
    elif All_Big(Cards):
        print('6')
        return [True,cards2string(Cards)]
    elif All_Small(Cards):
        print('7')
        return [True,cards2string(Cards)]
    elif CouYiSe(Cards):                ###考虑是否要特殊处理
        print('8')
        return [True,cards2string(Cards)]
    elif ShuangGuaiChongSan(Cards):     ###考虑是否要特殊处理
        print('9')
        return [True,cards2string(Cards)]
    elif SiTaoSanTiao(Cards):
        print('10')
        return [True,cards2string(Cards)]
    elif WuDuiSanTiao(Cards):
        print('11')
        return [True,cards2string(Cards)]
    elif LiuDuiBan(Cards):
        print('12')
        return [True,cards2string(Cards)]
    elif SanShunZi(Cards):
        print('13')
        return [True,cards2string(Cards)]
    elif SanTongHua(Cards):
        print('14')
        return [True,cards2string(Cards)]
    return [False,[]]

###########################################以上为特殊牌型判断###################################

###########################################以下为普通牌型判断###################################
from itertools import *

def quhuase(cards):#去花色
    new_cards = []
    for card in cards:
        card = card.strip('*')
        card = card.strip('#')
        card = card.strip('$')
        card = card.strip('&')
        new_cards.append(card)
    return new_cards

def isyidui(cards, l):          #是否为一对
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    duizi = []
    danpai = []
    danpai_num = 0
    duizi_num = 0
    for card in cards:
        card = card.strip('*')
        card = card.strip('#')
        card = card.strip('$')
        card = card.strip('&')
        card_dic[card] += 1
        if card_dic[card] == 1:
            danpai.append(card)
            danpai_num += 1
        elif card_dic[card] == 2:
            danpai.pop(-1)
            danpai_num -= 1
            duizi_num += 1
            duizi.append(card)
        elif card_dic[card] > 2:
            return [False, duizi, danpai]
    if l == 3:
        if danpai_num == 1 and duizi_num == 1:
            return [True, duizi, danpai]
    elif l == 5:
        if danpai_num == 3 and duizi_num == 1:
            return [True, duizi, danpai]
    return [False, duizi, danpai]

def isliangdui(cards):
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    duizi = []
    danpai = []
    danpai_num = 0
    duizi_num = 0
    for l in cards:
        l = l.strip('*')
        l = l.strip('#')
        l = l.strip('$')
        l = l.strip('&')
        card_dic[l] += 1
        if card_dic[l] == 1:
            danpai.append(l)
            danpai_num += 1
        elif card_dic[l] == 2:
            danpai.pop(-1)
            danpai_num -= 1
            if l in duizi:
                return [False, duizi, danpai]
            else:
                duizi_num += 1
                duizi.append(l)
        elif card_dic[l] > 2:
            return [False, duizi, danpai]
    if duizi_num == 2 and danpai_num == 1:
        return [True, duizi, danpai]
    return [False, duizi, danpai]

def issantiao(cards, l):       #是否为三条
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    danpai = []
    santiao = []
    danpai_num = 0
    santiao_num = 0
    for card in cards:
        card = card.strip('*')
        card = card.strip('#')
        card = card.strip('$')
        card = card.strip('&')

        card_dic[card] += 1
        if card_dic[card] == 1:
            danpai.append(card)
            danpai_num += 1
        elif card_dic[card] == 3:
            danpai.pop(-1)
            danpai_num -= 1
            santiao.append(card)
            santiao_num += 1
        elif card_dic[card] > 3:
            return [False, santiao, danpai]

    if l == 3:
        if santiao_num == 1 and danpai_num == 0:
            return [True, santiao, danpai]
    elif l == 5:
        if santiao_num == 1 and danpai_num == 2:
            return [True, santiao, danpai]
    return [False, santiao, danpai]

def isshunzi(cards):            #判断是否为顺子
    pai = ""
    for card in cards:
        card = card.strip('*')
        card = card.strip('#')
        card = card.strip('$')
        card = card.strip('&')
        pai += card
    if ShunZi_Judge(pai,len(cards)) > 0:
        return True
    return False

def istonghua(cards):           #是否为同花
    huase = []
    for card in cards:
        if card[0] not in huase:
            if len(huase) == 0:
                huase.append(card[0])
            else:
                return False
    return True

def ishulu(cards):              #判断是否为葫芦
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    duizi = []
    santiao = []
    duizi_num = 0
    santiao_num = 0
    for l in cards:
        l = l.strip('*')
        l = l.strip('#')
        l = l.strip('$')
        l = l.strip('&')
        card_dic[l] += 1
        if card_dic[l] == 2:
            duizi.append(l)
            duizi_num += 1
        elif card_dic[l] == 3:
            duizi.pop(-1)
            duizi_num -= 1
            santiao.append(l)
            santiao_num += 1
        elif card_dic[l] > 3:
            return [False, santiao, duizi]

    if santiao_num == 1 and duizi_num == 1:
        return [True, santiao, duizi]
    return [False, santiao, duizi]

def isboom(cards):              #判断是否为炸弹
    card_dic = {'2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0, 'J': 0, 'Q': 0, 'K': 0, 'A': 0}
    danpai = []
    boom = []
    danpai_num = 0
    boom_num = 0
    for l in cards:
        l = l.strip('*')
        l = l.strip('#')
        l = l.strip('$')
        l = l.strip('&')
        card_dic[l] += 1
        if card_dic[l] == 1:
            danpai.append(l)
            danpai_num += 1
        elif card_dic[l] == 4:
            danpai.pop(-1)
            danpai_num -= 1
            boom.append(l)
            boom_num += 1

    if boom_num == 1 and danpai_num == 1:
        return [True, boom, danpai]
    return [False, boom, danpai]

def istonghuashun(cards):       #判断是否为同花顺
    huase_dic = {'*':'','#':'','$':'','&':''}
    len_huase = {'*':0,'#':0,'$':0,'&':0}
    for card in cards:
        if card.find('*'):
            huase_dic['*'] += card.strip('*')
            len_huase['*'] += 1
        elif card.find('#'):
            huase_dic['#'] += card.strip('#')
            len_huase['#'] += 1
        elif card.find('$'):
            huase_dic['$'] += card.strip('$')
            len_huase['$'] += 1
        else:
            huase_dic['&'] += card.strip('&')
            len_huase['&'] += 1

    for key in huase_dic:
        if huase_dic[key] != '':
            if ShunZi_Judge(huase_dic[key],len_huase[key]):
                return True
    return False

def cards_type(cards):      #判断每墩牌型
    if len(cards) == 3:
        #判断是否为三条
        case1 = issantiao(cards, 3)
        if case1[0]:
            return ["三条", case1[1], case1[2]]

        # 判断是否为一对
        case2 = isyidui(cards, 3)
        if case2[0]:
            return ["一对", case2[1], case2[2]]
        #其余情况为散牌
        return ["散牌",cards]
    else:
        if istonghuashun(cards):
            return ["同花顺", cards]

        case1 = isboom(cards)
        if case1[0]:
            return ["炸弹", case1[1], case1[2]]

        case2 = ishulu(cards)
        if case2[0]:
            return ["葫芦", case2[1], case2[2]]

        if istonghua(cards):
            return ["同花",cards]

        if isshunzi(cards):
            return ["顺子",cards]

        case3 = issantiao(cards, 5)
        if case3[0]:
            return ["三条", case3[1], case3[2]]

        case4 = isliangdui(cards)
        if case4[0]:
            return ["两对", case4[1], case4[2]]

        case5 = isyidui(cards, 5)
        if case5[0]:
            return ["一对", case5[1], case5[2]]

        return ["散牌", cards]

def cards_cmp(cards1,cards2):  #牌型相同比较牌面大小
    card_dic = {'2':1,'3':2,'4':3,'5':4,'6':5,'7':6,'8':7,'9':8,'10':9,'J':10,'Q':11,'K':12,'A':13}
    cards1 = quhuase(cards1)
    cards2 = quhuase(cards2)
    l = min(len(cards1),len(cards2))
    for i in range(l - 1, -1, -1):
        if card_dic[cards1[i]] > card_dic[cards2[i]]:
            return -1
        elif card_dic[cards1[i]] < card_dic[cards2[i]]:
            return 1
    if len(cards1) > len(cards2):#较小墩牌较大
        return -1
    elif len(cards1) < len(cards2):#较小墩牌较小
        return 1
    return 0    #牌型相等

def liangdui_cmp(zhongdun_type,xiadun_type):#考虑连对情形
    res = 0
    zhongdun_duizi = []         #中墩对子
    xiadun_duizi = []           #下墩对子
    for duizi in quhuase(zhongdun_type[1]):
        zhongdun_duizi.append(duizi)
    for duizi in quhuase(xiadun_type[1]):
        xiadun_duizi.append(duizi)

    if zhongdun_duizi in [['2','3'],['3','4'],['4','5'],['5','6'],['6','7'],['7','8'],['8','9'],['9','10'],['10','J'],['J','Q'],['Q','K'],['K','J']]:
        if xiadun_duizi in [['2','3'],['3','4'],['4','5'],['5','6'],['6','7'],['7','8'],['8','9'],['9','10'],['10','J'],['J','Q'],['Q','K'],['K','J']]:
            if zhongdun_duizi == xiadun_duizi:  #连对相同比散牌
                res = cards_cmp(zhongdun_type[2],xiadun_type[2])
            else:                               #连对不相同比连对
                res = cards_cmp(zhongdun_type[1],xiadun_type[1])
        else:
            res = -1

    elif xiadun_duizi in [['2','3'],['3','4'],['4','5'],['5','6'],['6','7'],['7','8'],['8','9'],['9','10'],['10','J'],['J','Q'],['Q','K'],['K','J']]:
            res = 1
    else:
        cards1 = zhongdun_type[2]
        for card in zhongdun_type[1]:
            cards1.append(card)
        cards2 = xiadun_type[2]
        for card in xiadun_type[1]:
            cards2.append(card)
        res = cards_cmp(cards1, cards2)
    return res

#判断牌型是否合法，合法返回True及各墩牌型，否则返回False
def isleagle(cards):
    cards_order = {'散牌':0, '一对':1, '两对':2, '三条':3, '顺子':4, '同花':5, '葫芦':6, '炸弹':7, '同花顺':8}
    shangdun_type = cards_type(cards['shangdun'])
    zhongdun_type = cards_type(cards['zhongdun'])
    xiadun_type = cards_type(cards['xiadun'])

    if cards_order[shangdun_type[0]] < cards_order[zhongdun_type[0]]:
        res = 0
        if cards_order[zhongdun_type[0]] < cards_order[xiadun_type[0]]:
            return [True,[shangdun_type[0], zhongdun_type[0], xiadun_type[0]]]
        elif cards_order[zhongdun_type[0]]> cards_order[xiadun_type[0]]:
            return [False, []]
        else:
            cards1 = []
            cards2 = []
            if zhongdun_type[0] == "同花顺" or zhongdun_type[0] == "同花" or zhongdun_type[0] == "顺子" or zhongdun_type[0] == "散牌":
                cards1 = cards['zhongdun']
                cards2 = cards['xiadun']
                res = cards_cmp(cards1,cards2)
            elif zhongdun_type[0] != '两对':
                cards1 = zhongdun_type[2]
                for card in zhongdun_type[1]:
                    cards1.append(card)
                cards2 = xiadun_type[2]
                for card in xiadun_type[1]:
                    cards2.append(card)
                res = cards_cmp(cards1, cards2)
            else:
                res = liangdui_cmp(zhongdun_type,xiadun_type)

            if res == -1 or res == 0:
                return [False,[]]
            else:
                return [True, [shangdun_type[0], zhongdun_type[0], xiadun_type[0]]]

    elif cards_order[shangdun_type[0]] == cards_order[zhongdun_type[0]]:
        cards1 = copy(shangdun_type[1])
        cards2 = copy(zhongdun_type[1])
        if cards_cmp(cards1,cards2) == 1:
            if cards_order[zhongdun_type[0]] < cards_order[xiadun_type[0]]:
                return [True, [shangdun_type[0], zhongdun_type[0], xiadun_type[0]]]
            elif cards_order[zhongdun_type[0]] > cards_order[xiadun_type[0]]:
                return [False,[]]
            else:
                if zhongdun_type[0] == "同花顺" or zhongdun_type[0] == "同花" or zhongdun_type[0] == "顺子" or zhongdun_type[0] == "散牌":
                    cards1 = cards['zhongdun']
                    cards2 = cards['xiadun']
                    res = cards_cmp(cards1, cards2)
                elif zhongdun_type[0] != '两对':
                    cards1 = zhongdun_type[2]
                    for card in zhongdun_type[1]:
                        cards1.append(card)
                    cards2 = xiadun_type[2]
                    for card in xiadun_type[1]:
                        cards2.append(card)
                    res = cards_cmp(cards1, cards2)
                else:
                    res = liangdui_cmp(zhongdun_type, xiadun_type)
                if res == -1 or res == 0:
                    return [False, []]
                else:
                    return [True, [shangdun_type[0], zhongdun_type[0], xiadun_type[0]]]
        else:
            return [False, []]
    return [False,[]]

def Compare(old, new):
    cards_order = {'散牌':0, '一对':1, '两对':2, '三条':3, '顺子':4, '同花':5, '葫芦':6, '炸弹':7, '同花顺':8}

    leagle_new = isleagle(new)#判断牌型是否合法
    if not leagle_new[0]:#牌型不合法
        return [False,[]]
    else:
        new['type'] = leagle_new[1]
        type_old = old['type']
        type_new = new['type']

        #计算牌型净利润
        profit = 0

        # 计算下墩盈利
        p1 = abs(cards_order[type_old[2]] - cards_order[type_new[2]])
        if cards_order[type_old[2]] < cards_order[type_new[2]]:
            if type_new[2] == '同花顺':
                profit += 5 + p1
            elif type_new[2] == "炸弹":
                profit += 4 + p1
            else:
                profit += p1
        elif cards_order[type_old[2]] > cards_order[type_new[2]]:
            if type_new[2] == '同花顺':
                profit -= 5 + p1
            elif type_new[2] == "炸弹":
                profit -= 4 + p1
            else:
                profit -= p1
        else:
            res = 0
            card1 = cards_type(old['xiadun'])
            card2 = cards_type(new['xiadun'])
            if card1[0] == '两对':
                if card1[1] in [['2', '3'], ['3', '4'], ['4', '5'], ['5', '6'], ['6', '7'], ['7', '8'], ['8', '9'],
                                ['9', '10'], ['10', 'J'], ['J', 'Q'], ['Q', 'K'], ['K', 'J']]:
                    if card2[1] in [['2', '3'], ['3', '4'], ['4', '5'], ['5', '6'], ['6', '7'], ['7', '8'], ['8', '9'],
                                    ['9', '10'], ['10', 'J'], ['J', 'Q'], ['Q', 'K'], ['K', 'J']]:
                        res = cards_cmp(card1[1], card2[1])
                    else:
                        res = -1
                elif card2[1] in [['2', '3'], ['3', '4'], ['4', '5'], ['5', '6'], ['6', '7'], ['7', '8'], ['8', '9'],
                                    ['9', '10'], ['10', 'J'], ['J', 'Q'], ['Q', 'K'], ['K', 'J']]:
                    res = 1
                else:
                    res = cards_cmp(card1[1],card2[1])
            elif card1[0] == '三条' or card1[0] == '葫芦' or card1[0] == '炸弹':
                res = cards_cmp(card2[2], card1[2])
            else:
                res = cards_cmp(old['xiadun'], new['xiadun'])

            if res > 0:
                if type_new[1] == '同花顺':
                    profit += 10
                elif type_new[1] == "炸弹":
                    profit += 8
                elif type_new[1] == "葫芦":
                    profit += 2
                else:
                    profit += 1
            elif res < 0:
                if type_new[1] == '同花顺':
                    profit -= 10
                elif type_new[1] == "炸弹":
                    profit -= 8
                elif type_new[1] == "葫芦":
                    profit -= 2
                else:
                    profit -= 1

        # 计算中墩盈利
        p2 = abs(cards_order[type_old[1]] - cards_order[type_new[1]])
        if cards_order[type_old[1]] < cards_order[type_new[1]]:
            if type_new[1] == '同花顺':
                profit += 10 + p2
            elif type_new[1] == "炸弹":
                profit += 8 + p2
            elif type_new[1] == "葫芦":
                profit += 2
            else:
                profit += p2
        elif cards_order[type_old[1]] > cards_order[type_new[1]]:
            if type_new[1] == '同花顺':
                profit -= 10 + p2
            elif type_new[1] == "炸弹":
                profit -= 8 + p2
            elif type_new[1] == "葫芦":
                profit -= 2 + p2
            else:
                profit -= p2
        else:
            res = 0
            card1 = cards_type(old['zhongdun'])
            card2 = cards_type(new['zhongdun'])
            if card1[0] == '两对':
                if card1[1] in [['2', '3'], ['3', '4'], ['4', '5'], ['5', '6'], ['6', '7'], ['7', '8'],
                                ['8', '9'],
                                ['9', '10'], ['10', 'J'], ['J', 'Q'], ['Q', 'K'], ['K', 'J']]:
                    if card2[1] in [['2', '3'], ['3', '4'], ['4', '5'], ['5', '6'], ['6', '7'], ['7', '8'],
                                    ['8', '9'],
                                    ['9', '10'], ['10', 'J'], ['J', 'Q'], ['Q', 'K'], ['K', 'J']]:
                        res = cards_cmp(card1[1], card2[1])
                    else:
                        res = -1
                else:
                    res = cards_cmp(card2[2], card1[2])
            elif card1[0] == '三条' or card1[0] == '葫芦' or card1[0] == '炸弹':
                res = cards_cmp(card2[2], card1[2])
            else:
                res = cards_cmp(old['zhongdun'], new['zhongdun'])

            if res > 0:
                if type_new[1] == '同花顺':
                    profit += 10
                elif type_new[1] == "炸弹":
                    profit += 8
                elif type_new[1] == "葫芦":
                    profit += 2
                else:
                    profit += 1
            elif res < 0:
                if type_new[1] == '同花顺':
                    profit -= 10
                elif type_new[1] == "炸弹":
                    profit -= 8
                elif type_new[1] == "葫芦":
                    profit -= 2
                else:
                    profit -= 1

        #计算上墩盈利
        p3 = abs(cards_order[type_new[0]] - cards_order[type_old[0]])
        if cards_order[type_old[0]] < cards_order[type_new[0]]:
            profit += p3
        elif cards_order[type_old[0]] > cards_order[type_new[0]]:
            profit -= p3
        else:
            profit += cards_cmp(old['shangdun'], new['shangdun'])

        if profit > 0:
            return [True,new]
        else:
            return [False,[]]


#算法主体
def item_delete(items, cards):
    Cards = copy(cards)
    for item in items:
        Cards.remove(item)
    return Cards
#算法思想：利用组合的思想对所有牌型进行比较，择取最优牌型
def best_cards_type(cards):
    Cards_s = sort_Cards(cards)
    best_cards = {}
    cur_cards = {}

    #对手牌进行组合，并与当前最优牌型进行比较
    for shangdun in combinations(Cards_s,3):
        cur_shangdun = list(shangdun)
        cur_cards['shangdun'] = cur_shangdun
        Cards_ss = item_delete(cur_shangdun, copy(Cards_s))
        for zhongdun in combinations(Cards_ss, 5):
            cur_zhongdun = list(zhongdun)
            cur_cards['zhongdun'] = cur_zhongdun
            cur_xiadun  = item_delete(cur_zhongdun, copy(Cards_ss))
            cur_cards['xiadun'] = cur_xiadun
            cur_cards['type'] = ""
            if len(best_cards) == 0:
               if isleagle(cur_cards)[0]:
                   best_cards = copy(cur_cards)
                   best_cards['type'] = [cards_type(best_cards['shangdun'])[0], cards_type(best_cards['zhongdun'])[0],cards_type(best_cards['xiadun'])[0]]
            else:
                cur_res = Compare(best_cards,cur_cards)
                if cur_res[0]:
                    best_cards = copy(cur_res[1])

    shang = ""
    zhong = ""
    xia = ""
    best_cards_Type = []
    shang += best_cards['shangdun'][0] + ' ' + best_cards['shangdun'][1] + ' ' + best_cards['shangdun'][2]
    zhong += best_cards['zhongdun'][0] + ' ' + best_cards['zhongdun'][1] + ' ' + best_cards['zhongdun'][2] + ' ' + best_cards['zhongdun'][3] + ' ' + best_cards['zhongdun'][4]
    xia += best_cards['xiadun'][0] + ' ' + best_cards['xiadun'][1] + ' ' + best_cards['xiadun'][2] + ' ' + best_cards['xiadun'][3] + ' ' + best_cards['xiadun'][4]
    best_cards_Type.append(shang)
    best_cards_Type.append(zhong)
    best_cards_Type.append(xia)
    print(best_cards)
    return best_cards_Type


def shuafen():
    i = 10
    while i:
        info = startgame()
        if info['status'] == 0:
            postcards(info['data']['card'])
        i-=1

if __name__ == '__main__':
    login('dreamer', '147852369.6666')
    # shuafen()
    get_rank()