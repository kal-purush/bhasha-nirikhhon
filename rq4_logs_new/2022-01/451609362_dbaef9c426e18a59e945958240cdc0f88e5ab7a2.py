names = ["andrew", "kate", "sam", "edward", "Emma", "edwin"]

def print_upper_words(list, must_start_with={"e", "k"}):
    for element in list:
        if element.startswith(tuple(must_start_with)):
            print (element.upper())

print_upper_words(names, {"E", "a"})
