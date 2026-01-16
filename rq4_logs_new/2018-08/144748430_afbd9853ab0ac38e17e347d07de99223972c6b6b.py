name = input('Введите Ваше имя:')
player = {'Name':name,'HP': 100,'Dmg': 25,'Arm': 1.2}
enemy = {'Name':'Enemy','HP': 80,'Dmg': 13,'Arm': 1.2}
with open('player.txt', 'w+', encoding='utf-8') as file:
    for key, value in player.items():
        file.write(f'{key}: {value}\n')
with open('enemy.txt', 'w+', encoding='utf-8') as file:
    for key, value in enemy.items():
        file.write(f'{key}: {value}\n')
def game():
    gamecount = 1
    with open('player.txt', encoding='utf-8') as file:
        count = 1
        for line in file:
            if count == 1:
                sline = line.strip()
                sline = sline.split(': ')
                player[sline[0]] = sline[1]
            elif count == 4:
                sline = line.strip()
                sline = sline.split(': ')
                player[sline[0]] = float(sline[1])
            else:
                sline = line.strip()
                sline = sline.split(': ')
                player[sline[0]] = int(sline[1])
            count += 1
    with open('enemy.txt', encoding='utf-8') as file:
        count = 1
        for line in file:
            if count == 1:
                sline = line.strip()
                sline = sline.split(': ')
                enemy[sline[0]] = sline[1]
            elif count == 4:
                sline = line.strip()
                sline = sline.split(': ')
                enemy[sline[0]] = float(sline[1])
            else:
                sline = line.strip()
                sline = sline.split(': ')
                enemy[sline[0]] = int(sline[1])
            count += 1
    while enemy['HP'] > 0 and player['HP'] > 0:
        if gamecount % 2 == 0:
            attack(enemy,player)
        else:
            attack(player,enemy)
        gamecount += 1
    else:
        if enemy['HP'] <= 0 and player['HP'] > 0:
            print('Победил {}! У него осталось HP: {}! Прошло ходов: {}'.format(player['Name'],round(player['HP'],2),gamecount))
        elif player['HP'] <= 0 and enemy['HP'] > 0:
            print('Победил {}! У него осталось HP: {}! Прошло ходов: {}'.format(enemy['Name'],enemy['HP'],gamecount))



def new_dmg(player1, player2):
    dmg, arm = player1['Dmg'], player2['Arm']
    return dmg / arm

def attack(player1,player2):
    damage = new_dmg(player1,player2)
    new_hp = player2['HP'] - damage
    player2['HP'] = new_hp
game()