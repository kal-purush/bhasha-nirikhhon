#Comparator
#https://www.hackerrank.com/challenges/ctci-comparator-sorting/problem?h_l=interview&playlist_slugs%5B%5D=interview-preparation-kit&playlist_slugs%5B%5D=sorting

class Player:
    def __init__(self, name, score):
        self.name = name
        self.score = score
    def __repr__(self):
        pass
    def comparator(a, b):
        new_checker = Checker()
        return new_checker.compare(a,b)

class Comparator:
    def compare(a,b):
        pass

class Checker(Comparator):
    def __init__(self):
        pass

    def compare(self, a,b):
        diff = b.score - a.score
        if diff == 0:
            return -1 if a.name<b.name else 1
        return -1 if diff < 0 else 1


#pre-given driver
n = int(input())
data = []
for i in range(n):
    name, score = input().split()
    score = int(score)
    player = Player(name, score)
    data.append(player)
    
data = sorted(data, key=cmp_to_key(Player.comparator))
for i in data:
    print(i.name, i.score)

#sample input
# 5
# amy 100
# david 100
# heraldo 50
# aakansha 75
# aleksa 150

#sample output
# aleksa 150
# amy 100
# david 100
# aakansha 75
# heraldo 50