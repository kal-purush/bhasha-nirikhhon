import string

def getKey(item):
    return item[1]

with open("sample.txt") as opened_file:
         book = opened_file.read().lower()

for p in string.punctuation:
        book = book.replace(p, "")
        book = book.replace("  ", " ")
        book = book.replace("\n", " ")

book_histogram = {}
histogram_list = []

for word in book.split(" "):
    if word in book_histogram.keys():
        book_histogram[word] += 1
    else:
        book_histogram[word] = 1

for key, value in book_histogram.items():
     word_count = [key, value]
     histogram_list.append(word_count)

top_20 = sorted(histogram_list, key=getKey)[:-21:-1]
for key, value in top_20:
    print(key, value)


 #