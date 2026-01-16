import re
import random

def division(master):
     master = master.replace('-\n', '')
     master = master.replace('\n',' ')
     master = master.replace('-', ' -')
     while master != master.replace('  ', ' '):
         master = master.replace('  ', ' ')

     master = master.replace('(','')
     master = master.replace(')','')
     master = master.replace('[','')
     master = master.replace(']','')
     #master = master.replace('\','')
     master = master.replace('/','')
     master = master.replace('?..','?')
     master = master.replace('!..','!')
     master = master.replace('«', '')
     master = master.replace('»', '')
     master = master.replace('"','')
     master = master.replace('*','')

     master = master.replace('...','.')
     master = master.replace('.',' .')
     master = master.replace(',',' ,')
     master = master.replace('?',' ?')
     master = master.replace('!',' !')
     master = master.replace(':',' :')
     master = master.replace(';',' ;')
     master = master.replace(' .', ' . $')
     return master

def normalize(master):
     master = master.replace(' -', '-')
     while master != master.replace('  ', ' '):
         master = master.replace('  ', ' ')
     master = master.replace('$', '')
     master = master.replace(' .','.')
     master = master.replace(' ,',',')
     master = master.replace(' ?','?')
     master = master.replace(' !','!')
     master = master.replace(' :',':')
     master = master.replace(' ;',';')
     return master

def usual(sentense):
    while sentense[0] not in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ':
        sentense = sentense[1:]
    a = sentense[0].upper()
    sentense = a + sentense[1:]
    sentense = normalize(sentense)
    if sentense[len(sentense)-1] in '.?!':
        sentense = sentense[:-1]
    return sentense

def end(sentense):
    while sentense[0] not in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ':
        sentense = sentense[1:]
    a = sentense[0].upper()
    sentense = a + sentense[1:]
    sentense = normalize(sentense)
    while sentense[len(sentense)-1] not in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя.!?':
        sentense = sentense[:-1]
    if sentense[len(sentense)-1] not in '.?!':
        sentense = sentense + '.'
    return sentense

def bigramms(input, output):
    f = open(input, 'r')

    master = f.read()
    f.close()

    master = division(master)

    f = open(output, 'w')
    f.write(master)
    f.close()

    words = master.split(' ')

    c_words = {}

    for w in words:
        w = w.lower()
        if w in c_words.keys():
            c_words[w] +=1
        else:
            c_words[w] = 1

    c_bigs = {}

    for i in range(len(words)-1):
        a = words[i]
        b = words[i+1]
        w = a + ' ' + b
        w = w.lower()

        if w in c_bigs.keys():
            c_bigs[w] +=1
        else:
            c_bigs[w] = 1


    p_bigs = {}
    p_syllables5 = {}
    p_syllables7 = {}

    for w in c_bigs:
        [a,b] = w.split(' ')
        p_bigs[a + ' ' + b] = c_bigs[w]/c_words[a]

    return p_bigs


p_bigs = bigramms('Russian.txt', 'RussianNew.txt')

def trigramms(input, output):
    f = open(input, 'r')

    master = f.read()
    f.close()

    master = division(master)

    f = open(output, 'w')
    f.write(master)
    f.close()

    words = master.split(' ')

    c_words = {}

    for w in words:
        w = w.lower()
        if w in c_words.keys():
            c_words[w] +=1
        else:
            c_words[w] = 1

    c_bigs = {}

    for i in range(len(words)-1):
        a = words[i]
        b = words[i+1]
        w = a + ' ' + b
        w = w.lower()

        if w in c_bigs.keys():
            c_bigs[w] +=1
        else:
            c_bigs[w] = 1


    c_trigs = {}

    for i in range(len(words)-2):
        a = words[i]
        b = words[i+1]
        c = words[i+2]
        w = a + ' ' + b + ' ' + c
        w = w.lower()

        if w in c_trigs.keys():
            c_trigs[w] +=1
        else:
            c_trigs[w] = 1


    p_trigs = {}

    for w in c_trigs:
        [a,b,c] = w.split(' ')
        p_trigs[a + ' ' + b + ' ' + c] = c_trigs[w]/c_bigs[a + ' ' + b]

    return p_trigs

p_trigs = trigramms('Russian.txt', 'RussianNew.txt')


def string_syllables5():
    sylls_5 = []
    for w in p_bigs.keys():
        s = list(w)
        count = 0
        for h in s:
            if str(h) in 'аеёиоуыэюяАЕЁИОУЫЭЮЯ':
                count += 1
            else:
                pass
        if count == 5:
            sylls_5.append(w)
    for w in p_trigs.keys():
        s = list(w)
        count = 0
        for h in s:
            if str(h) in 'аеёиоуыэюяАЕЁИОУЫЭЮЯ':
                count += 1
            else:
                pass
        if count == 5:
            sylls_5.append(w)
    return sylls_5


def string_syllables7():
    sylls_7 = []
    for w in p_bigs.keys():
        s = list(w)
        count = 0
        for h in s:
            if str(h) in 'аеёиоуыэюяАЕЁИОУЫЭЮЯ':
                count += 1
            else:
                pass
        if count == 7:
            sylls_7.append(w)
    for w in p_trigs.keys():
        s = list(w)
        count = 0
        for h in s:
            if str(h) in 'аеёиоуыэюяАЕЁИОУЫЭЮЯ':
                count += 1
            else:
                pass
        if count == 7:
            sylls_7.append(w)
    return sylls_7


def create_haiku():
   a = usual(random.choice(string_syllables5())) + '\n' + usual(random.choice(string_syllables7())) + '\n' + end(random.choice(string_syllables5()))
   return a

def many_haiku(n):
    text = ''
    for i in range(n):
        haiku = create_haiku()
        text = text + '\n\nХайку ' + str(i+1) + '\n\n' + haiku
    text = text[1:]
    return text

n = int(input('Хайку: '))
haikus = many_haiku(n)
print(haikus)