# function to save list of words
def save_words(total_words):
    words = []
    for i in range(int(total_words)):
        words.append(input("Enter a word: "))
    return words
# function to count the number of words repeated
def count_repeat_one_word(words):
    word_count = {}
    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    return word_count
# function to convert list of total words to string
def convert_to_string(list_total_words):
    string = " ".join(map(str, list_total_words))
    return string

total_words = 0
list_words = []

total_words = input("Enter the number of words: ")
check_type = total_words.isnumeric()

if check_type == False:
    print("Please enter a number")
else:
    list_words = save_words(total_words)
    word_count = count_repeat_one_word(list_words)
    list_total_words = [word_count[key] for key in word_count]
    new_total_words_length = len(list_total_words)

    print(new_total_words_length)
    print(convert_to_string(list_total_words))