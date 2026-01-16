import random
from multiprocessing import Process


def test_stuff():
    total = 0
    i = ''
    myStr = "Hello World!"
    randomKeyList = []
    charList = []
    encryptedValues = []
    for i in myStr:
        i = ord(i)
        charList.append(i)
        n = random.randint(0, 1000000)
        randomKeyList.append(n)
        encryptedValues.append(n * i)
    for ele in range(0, len(encryptedValues)):
        total = total + encryptedValues[ele]
    print(total)


if __name__ == '__main__':
    p1 = Process(target=test_stuff)
    p1.start()
    p2 = Process(target=test_stuff)
    p2.start()

'''
keyValues = [1, 6, 5, 79, 8, 1, 2, 3]
string_to_encrypt = 'Hello There, 1234'
characterList = []


class EDAlgorithm:
    @staticmethod
    def encrypt():
        for i in string_to_encrypt:
            x = ord(i)
            characterList.append(hex(x))
        for

    @staticmethod
    def decrypt():
        back_to_string = []
        hex_list = []
        for hex_values in characterList:
            hex_values = int(hex_values, 16)
            hex_list.append(hex_values)
        for more_hex in hex_list:
            char_hex = chr(more_hex)
            back_to_string.append(char_hex)
        back_to_string = "".join(back_to_string)
        print(back_to_string)


def main():
    EDAlgorithm.encrypt()
    EDAlgorithm.decrypt()


main()
'''