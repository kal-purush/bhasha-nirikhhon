def main():
    meme = input('Enter the meme to use: ')

    meme_function = 0
    if meme == 'spaceout':
        meme_function = spaceout
    elif meme == 'alternating':
        meme_function = alternating
    elif meme == 'alternating2':
        meme_function = alternating2
    elif meme == 'lower':
        meme_function = lower
    elif meme == 'upper':
        meme_function = upper
    else:
        print('No such meme ' + meme)
        return

    while True:
        text = input()
        print(meme_function(text))

def lower(text):
    return text.upper()

def upper(text):
    return text.upper()

def spaceout(text):
    return ' '.join(list(text))

def alternating(text):
    text = text.lower()
    return_text = ""
    for i in range(len(text)):
        if i % 2 == 0:
            return_text += text[i]
        else:
            return_text += text[i].upper()
    return return_text

def alternating2(text):
    text = text.lower()
    return_text = ""
    for i in range(len(text)):
        if i % 2 == 1:
            return_text += text[i]
        else:
            return_text += text[i].upper()
    return return_text

main()