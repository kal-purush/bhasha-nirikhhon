# coding=utf-8
# 思路就是一副牌作为一个大的list，然后player1随机17张牌，list中删掉这17张牌，接着这样两次，最后剩下的就是手牌
# s for spade; h for heart; c for club; d for diamond
import random
remain_list = ['sA', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10', 'sJ', 'sQ', 'sK', \
               'hA', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8', 'h9', 'h10', 'hJ', 'hQ', 'hK', \
               'cA', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'cJ', 'cQ', 'cK', \
               'dA', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7', 'd8', 'd9', 'd10', 'dJ', 'dQ', 'dK', \
               'red Joker', 'black Joker']
# Method 1
player_1 = sorted(random.sample(remain_list,17))
for i in player_1:
    remain_list.remove(i)
player_2 = sorted(random.sample(remain_list,17))
for j in player_2:
    remain_list.remove(j)
player_3 = sorted(random.sample(remain_list,17))
for k in player_3:
    remain_list.remove(k)
print player_1,'\n',player_2,'\n',player_3,'\n',remain_list