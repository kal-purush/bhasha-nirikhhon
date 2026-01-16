from learn import SEQUENCES_NAME, pickle, array, INPUT_LENGTH, reshape
from random import randint
from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
import sys

# generate a sequence from a language model
def generate_seq(model, tokenizer, seq_length, seed_text, n_words):
    result = list()
    in_text = seed_text
    # generate a fixed number of words
    for _ in range(n_words):
        # encode the text as integer
        encoded = tokenizer.texts_to_sequences([in_text])[0]
        # print(in_text, encoded)

        # truncate sequences to a fixed length
        encoded = pad_sequences([encoded], maxlen=seq_length, truncating='pre')
        # predict probabilities for each word
        yhat = model.predict_classes(encoded, verbose=0)
        # map predicted word index to word
        out_word = ''
        for word, index in tokenizer.word_index.items():
            if index == yhat:
                out_word = word
                break
        # append to input
        in_text += '' + out_word
        result.append(out_word)
    return ''.join(result)

def print_error():
    print("Error – expecting:")
    print("`python3 create.py <'seedText'> <outputLength>`")
    print("or")
    print("`python3 create.py <'seedText'>`")
    print("or")
    print("`python3 create.py`")

def main():

    # print(str(sys.argv))
    # exit(0)

    if len(sys.argv) > 3:
        print('46')
        print_error()
        return
    elif len(sys.argv) == 3:
        print('50')
        gen_length = sys.argv[2]
        if not gen_length.isnumeric():
            print_error()
            return
        gen_length = int(gen_length)
        seed = sys.argv[1]

    elif len(sys.argv) == 2 and not sys.argv[1].isnumeric():
        print('59')
        gen_length = 500
        user_seed = sys.argv[1]
        user_seed = user_seed[:INPUT_LENGTH]
        seed = (INPUT_LENGTH - len(user_seed)) * ' ' + user_seed
    else:
        print('65')
        gen_length = 500
        sequences = pickle.load(open(SEQUENCES_NAME, "rb"))
        seed = sequences[randint(0, len(sequences))]
        assert len(seed) == INPUT_LENGTH

    print(gen_length)

    print(seed + "\n")

    tokenizerPath = 'tokenizer2.p'
    modelFilePath = 'model.h5'


    tokenizer = pickle.load(open(tokenizerPath, "rb"))
    model = load_model(modelFilePath)
    seq = generate_seq(model, tokenizer, INPUT_LENGTH, seed, gen_length)


    print()
    print(seq)


if __name__ == "__main__":
    main()