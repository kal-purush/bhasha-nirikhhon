from src.characters.senshi import Senshi
import random

class Tatakai:
    def iniciating(senshi1, senshi2):
        senshisOrder = []
        i = 0
        pool1 = senshi1.speed
        pool2 = senshi2.speed
        speed1 = 0
        speed2 = 0
        while (i <= pool1):
            speed1 += random.randint(1, 6)
            i += 1
        i = 0
        while (i <= pool2):
            speed2 += random.randint(1, 6)
            i += 1
        if (speed1 > speed2):
            senshisOrder.append(senshi1)
            senshisOrder.append(senshi2)
        elif (speed1 == speed2):
            if(random.randint(1, 2)%2 != 0):
                senshisOrder.append(senshi1)
                senshisOrder.append(senshi2)
            else:
                senshisOrder.append(senshi2)
                senshisOrder.append(senshi1)
        else:
            senshisOrder.append(senshi2)
            senshisOrder.append(senshi1)
        return senshisOrder

    def fullBattle(senshi1, senshi2):
            if (not senshi1.name == "" and not senshi2.name == ""):
                round = 1
                while(senshi1.health > 0 and senshi2.health > 0):
                    print ("\n rodada ",  round, "\n")
                    senshisOrder = Tatakai.iniciating(senshi1, senshi2)
                    first = senshisOrder[0]
                    last = senshisOrder[1]
                    first.healing()
                    damage = first.attacking()
                    defended = last.defending()
                    last.hurted(damage, defended, first)
                    print(first.name, " atacou com ", damage, " de dano a ", last.name)
                    if (defended >= damage):
                        print(last.name, " defendeu perfeitamente o ataque de ", first.name)
                    else:
                        print(last.name, " defendeu ", defended, " do ataque de ", first.name)
                    print(last.name, " está com ", last.health, " de saúde")
                    if (last.health <= 0):
                        break
                    last.healing()
                    damage = last.attacking()
                    defended = first.defending()
                    first.hurted(damage, defended, last)
                    print(last.name, " atacou com ", damage, " de dano a ", first.name)
                    if (defended >= damage):
                        print(first.name, "defendeu perfeitamente o ataque de ", last.name)
                    else:
                        print(first.name, " defendeu ", defended, " do ataque de ", last.name)
                    print(first.name, " está com ", first.health, " de saúde")
                    round += 1

                if (senshi1.health<= 0):
                    print(senshi2.name, " venceu")
                else:
                    print(senshi1.name, " venceu")
            else:
                print("Apenas verdadeiros senshis podem participar do tatakai");

    def simpleBattle(senshi1, senshi2):
            if (not senshi1.name == "" and not senshi2.name == ""):
                round = 1
                while(senshi1.health > 0 and senshi2.health > 0):
                    senshisOrder = Tatakai.iniciating(senshi1, senshi2)
                    first = senshisOrder[0]
                    last = senshisOrder[1]
                    first.healing()
                    damage = first.attacking()
                    defended = last.defending()
                    last.hurted(damage, defended, first)
                    if (last.health <= 0):
                        break
                    last.healing()
                    damage = last.attacking()
                    defended = first.defending()
                    first.hurted(damage, defended, last)
                    round += 1

                if (senshi1.health <= 0):
                    print(senshi2.name, " venceu ", senshi1.name , " em " , round, " rodadas com  ", senshi2.health, " pontos de vida ")
                    return senshi2
                else:
                    print(senshi1.name, " venceu ", senshi2.name, " em ", round, " rodadas com  ", senshi1.health, " pontos de vida ")
                    return senshi1
            else:
                print("Apenas verdadeiros senshis podem participar do tatakai");

    def groupBattle(senshisIn, senshisOut):
        if (len(senshisIn)%2 == 0):
            while (len(senshisIn) > 0):
                index = random.randint(0, (len(senshisIn) - 1))
                senshiX = senshisIn[index]
                senshisIn.remove(senshiX)
                index = random.randint(0, (len(senshisIn) - 1))
                senshiY = senshisIn[index]
                senshisIn.remove(senshiY)
                senshisOut.append(Tatakai.simpleBattle(senshiX, senshiY))
            for senshi in senshisOut:
                senshi.health = senshi.maxHealth
                senshi.levelingUp();
        else:
            print("o grupo precisa ter uma quantidade par de senshis")

    def tournament(senshis: []):
        octaves = senshis
        quarters = []
        semi = []
        final = []
        print("\n Oitavas de Final \n")
        Tatakai.groupBattle(octaves, quarters)
        print("\n Quartas de Final \n")
        Tatakai.groupBattle(quarters, semi)
        print("\n Semifinal \n")
        Tatakai.groupBattle(semi, final)
        print("\n Final \n")
        Tatakai.simpleBattle(final[0], final[1])