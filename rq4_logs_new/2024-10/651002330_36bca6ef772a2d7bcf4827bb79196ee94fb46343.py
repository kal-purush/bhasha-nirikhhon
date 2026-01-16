def count_unique_words(input_string):
    words = input_string.lower().split()

    unique_words = set(words)
    return len(unique_words)