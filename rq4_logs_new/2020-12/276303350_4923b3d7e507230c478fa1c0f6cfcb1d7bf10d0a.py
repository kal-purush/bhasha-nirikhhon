import random
import requests
import re
import xml.etree.ElementTree as ET
from parsers.parse import parseTenhou

class TableRate():

    def __init__(self, rate=0.3, shugi=.50, target=30000, start=25000, uma=[30,10,-10,-30]):
        self.rate = rate
        self.shugi = shugi
        self.oka = 0
        self.target = target
        self.start = start
        self.uma = uma

    def __eq__(self, other):
        return (self.rate == other.rate and self.shugi == other.shugi and self.oka == other.oka and self.target == other.target and self.start == other.start and self.uma == other.uma)

    def __str__(self):
        return f"Rate: {self.rate}, Start: {self.start}, Target: {self.target}, Shugi: {self.shugi}, Oka: {self.oka}, Uma: {self.uma}"

# default values rate=0.3, shugi=.50, target=30000, start=25000, uma=[30,10,-10,-30]    
TENSAN = TableRate()
TENGO = TableRate(rate=0.5, shugi=1)
TENPIN = TableRate(rate=1, shugi=2)

def scorePayout(players, tableRate):

    players.sort(key=lambda x: x[1],reverse=True)

    lastScore = ""
    lastScore += f"{tableRate}\n\n"
    lastScore += f"All players begin with **Start**[{tableRate.start}] points and pay the difference of the **Target**[{tableRate.target}] to first place\n" 
    lastScore += f"So first place gets an **Oka** of ({tableRate.start} - {tableRate.target}) × {len(players)} = {(tableRate.target - tableRate.start)*len(players)}\n\n"
    lastScore += f"A **Rate** of {tableRate.rate} means each 1000 points is ${tableRate.rate:.2f}\n\n"

    oka = [tableRate.oka] + [0]*len(players) # giving 1st place oka bonus
    for i, p in enumerate(players):
        name, score, shugi = p
        lastScore += f"#{i+1}Place: (Score[{score}] + Oka[{oka[i]}] - Target[({tableRate.target}])/1000) × Rate[{tableRate.rate}] = ${((score + oka[i] - tableRate.target)/1000)*tableRate.rate:.2f}\n"

    lastScore += "\n"            
    lastScore += f"**Uma** of {tableRate.uma} is also applied based on position, and multiplied by the table **Rate**[{tableRate.rate}]\n"

    for i, p in enumerate(players):
        lastScore += f"#{i+1}Place: ${tableRate.rate*tableRate.uma[i]}\n"

    lastScore += "\n"
    lastScore += f"Finally the **Shugi Rate**[{tableRate.shugi}] is multiplied by the number of shugi and added to the final score\n"

    for i,p in enumerate(players):
        lastScore += f"#{i+1}Place: Shugi rate[{tableRate.shugi}] × Player Shugi[{shugi}] = {tableRate.shugi * shugi}\n"

    lastScore += "\n"
    lastScore += "Finally\n"

    ret = []
    for i, p in enumerate(players):

        name, score, shugi = p
        score = score*100
        calc = (((score + oka[i] - tableRate.target)/1000) + tableRate.uma[i]) * tableRate.rate + tableRate.shugi * shugi
        payout = round(calc,2)
        ret.append([name,score,shugi,payout])
        lastScore += f"#{i+1}Place: (((({score}+{oka[i]}-{tableRate.target})/1000)+{tableRate.uma[i]})×{tableRate.rate}+{tableRate.shugi}×{shugi}) = ${payout}\n"

    return [ret, lastScore]