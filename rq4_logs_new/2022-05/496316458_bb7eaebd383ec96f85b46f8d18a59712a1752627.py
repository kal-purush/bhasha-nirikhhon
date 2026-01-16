import sys
import numpy as np
from map_reduce import map_reduce
from data_parser import data_parser
from mappers_and_reducers import bigram_emitter, bigram_reducer, reduce_to_matrix, no_reduce, no_map


def classifier(lang1, lang2, filename):
    """
    Classifies the language of the lines in the given file.
    :param lang1:
    :param lang2:
    :param filename:
    :return: List of tuples of the form (lang1, count), (lang2, count)
    """
    #load the data
    name_string = f'identification_matrix_{lang1}_{lang2}'
    print(f'Loading {name_string}...')
    try:
        identification_matrix = np.load(name_string+'.npy')
    except FileNotFoundError:
        print('No file found. Please run this program by executing main.py.')
        sys.exit()

    #parse the data
    print('Parsing data...')
    data_parsed = data_parser(filename, bigram_split=True, line_split=True)
    #map reduce into bigram frequency counts
    reduced_lines = list(map_reduce(bigram_emitter, bigram_reducer, i) for i in data_parsed) #reduce to bigrams

    #classify the lines
    print('Classifying...')
    classified_lines = []
    for line in reduced_lines: #for each line
        frequency = dict(line) #make a dictionary of bigram frequencies per line
        matrices = list(map_reduce(no_map, reduce_to_matrix, list(frequency.items()))) #make a list of matrices
        combined_matrices = np.sum(matrices, axis=0) #sum the matrices
        line_score = np.sum(np.multiply(identification_matrix, combined_matrices)) #multiply the occurences of bigrams in the text with the "score" of the bigram, and sum the results
        if line_score > 0: #if the score is positive, the text is in the positive language
            classified_lines.append(lang1)
        elif line_score < 0: #if the score is negative, the text is in the negative language
            classified_lines.append(lang2)
        else:
            classified_lines.append('unknown')
    results = list(map_reduce(bigram_emitter, bigram_reducer, classified_lines))
    print('Done!')
    print('---- Results ----')
    print(f'{results[0][0]}: {results[0][1]} lines')
    print(f'{results[1][0]}: {results[1][1]} lines')

