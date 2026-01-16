#CSCI 1913 Khalid Mohamed Project1 moha0768

class Random:
    def __init__(self,seed):
        self.seed = seed

    def next(self):
        self.seed = ((16807) * self.seed) % (2147483647)# hard coded minus 1
        return self.seed

    def choose(self,limit):
        nextt = self.next()
        return nextt % limit

class Rule:
    def __init__(self,left, right):
        self.left = left
        self.right = right
        self.count = 1

    def __repr__(self):
        stringA = " "
        stringA += str(self.count) + " " + self.left + " " + " -->"
        for i in self.right:
            stringA += " " + i
            return stringA


class Grammar():
    def __init__(self,seed):
        self.randomnum = Random(seed)
        self.dictionary = {}

    def rule(self, left, right):
        if left in self.dictionary:
            self.dictionary[left] += (Rule(left, right),)
        else:
            self.dictionary[left] = (Rule(left, right),)


    def generate(self):
        if 'Start' in self.dictionary:
            return self.generating(('Start',))
        else:
            raise Exception("Can not generate strings")

    def generating(self, strings):
        result = ""
        for x in strings:
            if x not in self.dictionary:
                result += x + " "
            else:
                nonterminal = self.select(x)
                result += self.generating(nonterminal.right)
        return result

    def select(self, left):
        rules = self.dictionary[left]
        total = 0
       # print (self.dictionary)
        for i in rules:
            total += i.count
        index = self.randomnum.choose(total)
        for i in rules:
            index = index - i.count
            if 0 >= index:
                chosen = i
                break
        for i in rules:
            if i != chosen:
                i.count +=1
        return chosen

G = Grammar(101)
G.rule('Noun',   ('cat',))                                #  01
G.rule('Noun',   ('boy',))                                #  02
G.rule('Noun',   ('dog',))                                #  03
G.rule('Noun',   ('girl',))                               #  04
G.rule('Verb',   ('bit',))                                #  05
G.rule('Verb',   ('chased',))                             #  06
G.rule('Verb',   ('kissed',))                             #  07
G.rule('Phrase', ('the', 'Noun', 'Verb', 'the', 'Noun'))  #  08
G.rule('Story',  ('Phrase',))                             #  09
G.rule('Story',  ('Phrase', 'and', 'Story'))              #  10
G.rule('Story',  ('Phrase', 'but', 'Story'))              #  11
G.rule('Start',  ('Story', '.'))                          #  12

r = Rule("left", ('qw', 'ew'))