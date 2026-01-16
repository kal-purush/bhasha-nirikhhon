import pandas

data = pandas.read_csv("F:/Kushal/Programing/Python Programing/basic/NATO-alphabet-start/nato_phonetic_alphabet.csv")

phonetic_dict = {row.letter: row.code for (index, row) in data.iterrows()}
print(phonetic_dict)

def genrate_phonetic():
    word = input("Enter a word: ").upper()
    try:
        output_list = [phonetic_dict[letter] for letter in word]
    except KeyError:
        print("Sorry! only letters on the alphabet please.")
        genrate_phonetic()
    else:
        print(output_list)

genrate_phonetic()