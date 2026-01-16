import random


def embaralha(string):
    random.shuffle(string)
    palavra_embaralhada = ''.join(string)
    return palavra_embaralhada


if __name__ == '__main__':
    palavra = str(input('Digite uma palavra: ').lower())
    s = []
    for _ in palavra:
        s.append(_)
    print(embaralha(s))