from random import shuffle, choice
from sys import path
path = path[0][9:] + "/DB/db/"
path_rg = f"{path}People_register.txt"

def seePath():
    print(path)

def people_register(seed=False):
    if seed == False: users = []
    try:
        print(path_rg)
        with open(path_rg, "r") as f:
            for n, line in enumerate(f):
                if seed:
                    line = line[:-1].split(";")
                    print(f"{n+1}- {line[0]:<30}{line[1]:>3}")
                else:
                    users.append(line[:-1])
        if seed == False:
            user_dict = {}
            shuffle(users)
            for l in users:
                line = l.split(";")
                user_dict[line[0]] = line[1]
            return user_dict
    except FileNotFoundError:
        print("\033[31mNão foi possível encontrar lista de usuários\033[m")
    except Exception as e:
        print(e)

def people_register_add(username):
    try:
        with open(path_rg, "a") as f:
            password = input("Senha: ")
            f.write(f"{username};{password}\n")
    except Exception as e:
        print(e)

def people_register_rm():
    people_register(True)
    '''Elimina uma pessoa da lista em .txt
    Rcecebe o nome de uma lista de formato .txt
    Chama função transaction'''
    user = []
    try:
        username = input("Digite o usuário: ")
        with open(path_rg, "r") as f:
            for line in f:
                line = line.split(";")
                if username != line[0]:
                    user.append(f"{line[0]};{line[1]}")
        with open(path_rg, "w") as f:
            for line in user:
                f.write(f"{line}")
        hashtag_rm_list(username)
        comment_rm_list(username)
    except FileNotFoundError:
        print(f"\033[31mNão foi possível encontrar {username}\033[m")
    except Exception as e:
        print(e)

def hashtag_list(username):
    tag = []
    try:
        with open(f"{path}ht-{username}.txt", "r") as f:
            for line in f:
                tag.append(line[:-1])
        return choice(tag)
    except FileNotFoundError:
        print("\033[31mNão foi possível encontrar lista de hashtag\033[m")

def hashtag_read(username):
    with open(f"{path}ht-{username}.txt", "r") as f:
        for cont, ht in enumerate(f):
            print(f"#{ht}", end="")
        print(f"Total de hashtags {cont}")

def hashtag_add(username):
    tags = []
    with open(f"{path}ht-{username}.txt", "a") as f:
        print("Para sair basta apertar enter")
        while True:
            name = input("Nome de hashtag: ")
            if name == "":
                break
            else:
                tags.append(name)
        for line in tags:
            f.write(f"{line}\n")

def hashtag_rm(username):
    word_list = []
    with open(f"{path}ht-{username}.txt", "r") as f:
        for n, line in enumerate(f):
            print(f"{n+1}- {line}", end="")
            word_list.append(line)
    answer = int(input("Remover qual opção? "))
    answer -= 1
    word_list.pop(answer)
    with open(f"{path}ht-{username}.txt", "w") as f:
        for line in word_list:
            f.write(line)

def hashtag_rm_list(username):
    from os import remove
    try:
        remove(f"{path}ht-{username}.txt")
    except FileNotFoundError as e:
        print(e)

def comment_list(username):
    box_words = []
    try:
        with open(f"{path}w-{username}.txt", "r") as f:
            for line in f:
                box_words.append(line[:-1])
        return choice(box_words)
    except FileNotFoundError:
        print("\033[31mNão foi possível encontrar lista de comentários\033[m")

def comment_read(username):
    with open(f"{path}w-{username}.txt", "r") as f:
        for cont, ht in enumerate(f):
            print(ht, end="")
        print(f"Total de comentários {cont}")

def comment_add(username):
    tag = []
    with open(f"{path}w-{username}.txt", "a") as f:
        print("Para sair basta apertar enter")
        while True:
            name = input("Comentário: ")
            if name == "":
                break
            else:
                tag.append(name)
        for line in tag:
            f.write(f"{line}\n")

def comment_rm(username):
    word_list = []
    with open(f"{path}w-{username}.txt", "r") as f:
        for n, line in enumerate(f):
            print(f"{n+1}- {line}", end="")
            word_list.append(line)
    answer = int(input("Remover qual opção? "))
    answer -= 1
    word_list.pop(answer)
    with open(f"{path}w-{username}.txt", "w") as f:
        for line in word_list:
            f.write(line)

def comment_rm_list(username):
    from os import remove
    try:
        remove(f"{path}w-{username}.txt")
    except FileNotFoundError as e:
        print(e)
