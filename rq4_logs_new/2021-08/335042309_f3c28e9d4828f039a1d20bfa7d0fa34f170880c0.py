def fill_with_space(text, k, islast):
    # Here text is a string with words separated with spaces
    if len(text.split())==1 or islast:
        # Here only one word is there or last line, so we pad right with space
        text = text + " "*(k-len(text))
        return text

    # In this case, text has more than one word.
    spaces = len(text.split()) - 1 # Counts number of spaces within words
    q = (k-len(text))//spaces
    r = (k-len(text))%spaces
    words = text.split()
    out = words[0]
    for i in range(1,len(words)):
        if r>0:
            out = out + " "*(q+2) + words[i]
            r -= 1
        else:
            out = out + " "*(q+1) + words[i]
    # out += " " + words[-1]
    return out

# print(fill_with_space("the lazy dog", 16))
def justify(words, k):

    # char_count = 0
    # word_count = 0
    # words = text.split()
    output = []
    i = 0
    temp = "" # Contains current line

    while i<len(words):
        # We maintain a counter for number of words we fitted in current line till now and one for number of characters
        if temp=="":
            # If temp is starting of a new line
            temp += words[i]
            i += 1
            continue

        if len(temp)+len(words[i])>k:
            # If temp already has enough characters, we just fill the rest up with spaces and go to next line
            temp = fill_with_space(temp, k, False)
            output.append(temp)
            temp = ""
            continue
        else:
            # Current line has enough characters left, we move to next word
            temp += " " + words[i]
            i += 1
    output.append(fill_with_space(temp, k, True))

    return(output)

def print_format(out):
    for o in out:
        print(o)
        print("\n")

# justify("The quick brown fox jumps over the", 16)

justify(["What","must","be","acknowledgment","shall","be"], 16)

justify(["Science","is","what","we","understand","well","enough","to","explain","to","a","computer.","Art","is","everything","else","we","do"],20)