from random import shuffle

def loadPlayers():
    players = []
    
    with open("players.txt") as file:
        players = file.read().splitlines()
    
    return players

def main():
    players = loadPlayers()
    shuffle(players)
    
    targets = players[:]
    total = len(players)

    lines = []
    
    for i in range(total):
        lines.append(players[i] + "'s target is " + targets[(i+1) % total] + ".\n")
        
    with open("targets.txt", "w") as file:
        file.write("-----------SORTED ALPHABETICALLY----------\n")
        lines = sorted(lines, key=str.lower)
        for line in lines:
            file.write(line)
        file.write("------------------------------------------")
    
main()