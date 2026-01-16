# jogo-forca
from random import randint

#menu
print(" ")
print("🌺🐶🐕🐩🌺")
print("JOGO DA FORCA")
print("Tema: raças de cachorros")
print("🌺🐶🐕🐩🌺")
print(" ")

#variáveis
words = ["husky", "golden", "akita", "beagle", "bulldog", "corgi", "poodle", "pitbull", "pug", "collie", "schnauzer", "doberman"]
lifes = 7
random = randint(0, 11)

word = words[random]
wordProgress = (["_"] * len(word))
numberWords = ("_"*len((words[random])))
print ("Vidas = 7")
print (f"🦴Quantidade de Letras: {numberWords}")

while lifes > 0:
    letter = input("Digite uma letra: ")

#bonequinho

    if lifes == 6:
        print("🦴Bonequinho:")
        print(" o ")
        print(" |")
        print(" ")

    elif lifes == 5:
        print("🦴Bonequinho:")
        print(" o ")
        print("/| ")
        print(" ")

    elif lifes == 4:
        print("🦴Bonequinho:")
        print(" o ")
        print("/|\ ")
        print(" ")

    elif lifes == 3:
        print("🦴Bonequinho:")
        print(" o ")
        print("/|\ ")
        print("/")

    elif lifes == 2:
        print("🦴Bonequinho:")
        print(" o ")
        print("/|\ ")
        print("/ \ ")

    elif lifes == 1:
        print("🦴Bonequinho:")
        print(" o ")
        print("/|\ ")
        print("/ \ ")

    elif lifes == 0:
        print("🦴Bonequinho:")
        print(" o ")
        print (" - ")
        print("/|\ ")
        print("/ \ ")


#execução
    if letter in word:
        print("Correto!")
        print(" ")
        for i in range(len(word)):
            if word[i] == letter:
                wordProgress[i] = letter
        print("".join(wordProgress))
        print(f"Vidas Restantes = {lifes}")

    else:
        print("Incorreto!")
        print(" ")
        lifes -= 1
        print(f"Vidas Restantes = {lifes}")

    if "_" not in wordProgress:
        print("Parabéns! Você sobreviveu.")
        print (f"Seu cachorrinho é o: {word}")
        print ("🦴FIM🦴")
        print("🌺🐶🐕🐩🌺")
        break

    if lifes == 0:
        print("Bonequinho:")
        print(" o ")
        print(" - ")
        print("/|\ ")
        print("/ \ ")
        print("Descanse em paz...")
        print ("GAME OVER")
        print("🌺🐶🐕🐩🌺")
        break