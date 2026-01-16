import numpy as np

# Inference of new sentence given trained LSTM model and new image
def sampling(image_features, LSTM, max_length, word2int):


    EOS = False
    iter_count = 0

    pad_length = 60
    sentence = np.zeros(pad_length)

    # Generate new words based on probabilities
    while not EOS:

        # TODO: Implement trained LSTM model and infere p_n of next word to produce most likely int
        produced_int = 321  # Dummy inference

        sentence[iter_count] = produced_int

        if (iter_count > max_length):
            EOS = True
        elif (produced_int == word2int['</s>']):
            EOS = True
        else:
            iter_count += 1

    output = np.hstack((image_features, sentence))

    return output


sentence = sampling(555555, 2, 15, 5)

print sentence