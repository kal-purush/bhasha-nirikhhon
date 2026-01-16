import random
pull_word_index = []
file = open("dic.txt","r")
words = list(file.read().split())

sel_word = list(str(words[random.randint(0,len(words)-1)]))

strip_word = list(sel_word)

pull_word_index = sorted(random.sample(range(0,len(sel_word)), 3))

ctr_index = list(pull_word_index)

for i in pull_word_index:
    strip_word[i] = "_"

print(''.join(strip_word))

kill_him = ["H","A","N","G","E","D"]
fail_count = 0

def game_engine(food):
    global fail_count
    found = 0
    for k in ctr_index:
        if user_input in sel_word[k]:
            found = 1
            strip_word[k] = food
    print(''.join(strip_word))
    if found != 1:
        fail_count = fail_count + 1
    if fail_count == 1:
        print(kill_him[0])
    else:
        print(''.join(kill_him[:fail_count]))
    
while sel_word != strip_word:
    user_input = input("Enter the Missing letter: ")
    game_engine(user_input)
    if fail_count == 6:
        break
    if strip_word == sel_word:
        print("congratulation !")
        break
    