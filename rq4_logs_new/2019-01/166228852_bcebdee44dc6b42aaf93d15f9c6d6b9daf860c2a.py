#!/usr/bin/python
# -*- coding: utf-8 -*-

import random
import discord
import sys
from configparser import ConfigParser
from pprint import pprint

config = ConfigParser()
config.read('./randomizer.ini')

def readFileToList(path):
	list = []
	f = open(path,'r')
	for line in f:
		line = line.strip()
		if line.startswith('#'):
			continue
		if len(line) == 0:
			continue
		list.append(line)
	f.close()
	return list

def getWeponTypeList():
    wepon_set = []
    wepon_type_list = ["blaster", "charger", "manuver", "roller_brush", "shelter", "shooter", "slosher", "splatling"]
    for i in range(len(wepon_type_list)) :
        wepon_line = []
        for j in range(4) :
            if i + j < len(wepon_type_list) :
                wepon_line.append(wepon_type_list[i + j])
            else :
                wepon_line.append(wepon_type_list[i + j + 1 -len(wepon_type_list)])
        wepon_set.append(wepon_line)
    return wepon_set

def choiceWepon(wepon_type):
    wepon_set = []
    for i in wepon_type :
        wepon_list = []
        for j in i :
            path = "./wepons/" + j + "_file.txt"
            wepon_list.append(random.choice(readFileToList(path)))
        wepon_set.append(wepon_list)
    return wepon_set

def init_result():
    global result_set
    result_set = []
    rule_list = ["ナワバリ", "ガチエリア", "ガチアサリ", "ガチホコ", "ガチヤグラ"]
    random.shuffle(rule_list)
    wepon_set = getWeponTypeList()
    game_num = 5
    alpha_wepon_type = random.sample(wepon_set, game_num)
    bravo_wepon_type = random.sample(wepon_set, game_num)

    alpha_wepon_list = choiceWepon(alpha_wepon_type)
    bravo_wepon_list = choiceWepon(bravo_wepon_type)

    result_set.append(rule_list)
    result_set.append(alpha_wepon_list)
    result_set.append(bravo_wepon_list)
    return

def setAlphaMember(name):
    path = "./teams/" + name + ".txt"
    global alpha_member
    alpha_member = readFileToList(path)

def setBravoMember(name):
    path = "./teams/" + name + ".txt"
    global bravo_member
    bravo_member = readFileToList(path)

def setTeamMember(name):
    path = "./teams/" + name + ".txt"
    return readFileToList(path)

def getOutputText(wepon_list, member_list):
    text = ""
    for i in range(len(member_list)) :
        text = text + member_list[i] + " : " + wepon_list[i] + "\n"
    return text

def getResultSet():
    global result_set
    return result_set

def getAlphaMembers():
    global alpha_member
    return alpha_member
        
def getBravoMembers():
    global bravo_member
    return bravo_member


client = discord.Client()
alpha_member = []
bravo_member = []
result_set = []

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')

@client.event
async def on_message(message):
    # reset,teamA,teamB という入力前提 
    # うまいやり方があれば変更する
    if message.content.startswith('reset,'):
        if message.channel.id == config['text_id']['general'] :
            contents = message.content.split(",")
            setAlphaMember(contents[1])
            setBravoMember(contents[2])
            init_result()
            reply = 'ランダマイザーの内容をリセットしました'
            await client.send_message(message.channel, reply)

    if message.content.startswith('1st') or message.content.startswith('2nd') or message.content.startswith('3rd') or message.content.startswith('4th') or message.content.startswith('5th') :
        if message.channel.id == config['text_id']['general'] :
            game_num = int(message.content[0])
            result_set = getResultSet()
            rule_text = str(game_num) + "試合目のバトルルール : " + result_set[0][game_num - 1] + "\n"
            await client.send_message(client.get_channel(config['text_id']['general']), rule_text)
            alpha_text = str(game_num) + "試合目のアルファチームの武器\n" + getOutputText(result_set[1][game_num - 1], getAlphaMembers())
            await client.send_message(client.get_channel(config['text_id']['alpha']), alpha_text)
            bravo_text = str(game_num) + "試合目のブラボーチームの武器\n" + getOutputText(result_set[2][game_num - 1], getBravoMembers())
            await client.send_message(client.get_channel(config['text_id']['bravo']), bravo_text)

client.run(config['access_token']['token'])