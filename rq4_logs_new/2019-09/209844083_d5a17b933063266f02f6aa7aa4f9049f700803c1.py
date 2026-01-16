from Viterbi import most_likely_tagging


tag_set = {
    'NNP', 'MD', 'VB', 'JJ', 'NN', 'RB', 'DT'
}

tag_tag_conditional = {
    '<s>': {'NNP': 0.2767, 'MD': 0.0006, 'VB': 0.0031, 'JJ': 0.0453, 'NN': 0.0449, 'RB': 0.051, 'DT': 0.2026},
    'NNP': {'NNP': 0.3777, 'MD': 0.011, 'VB': 0.0009, 'JJ': 0.0084, 'NN': 0.0584, 'RB': 0.009, 'DT': 0.0025},
    'MD': {'NNP': 0.0008, 'MD': 0.0002, 'VB': 0.7968, 'JJ': 0.0005, 'NN': 0.0008, 'RB': 0.1698, 'DT': 0.0041},
    'VB': {'NNP': 0.0322, 'MD': 0.0005, 'VB': 0.005, 'JJ': 0.0837, 'NN': 0.0615, 'RB': 0.0514, 'DT': 0.2231},
    'JJ': {'NNP': 0.0366, 'MD': 0.0004, 'VB': 0.0001, 'JJ': 0.0733, 'NN': 0.4509, 'RB': 0.0036, 'DT': 0.0036},
    'NN': {'NNP': 0.0096, 'MD': 0.0176, 'VB': 0.0014, 'JJ': 0.0086, 'NN': 0.1216, 'RB': 0.0177, 'DT': 0.0068},
    'RB': {'NNP': 0.0068, 'MD': 0.0102, 'VB': 0.1011, 'JJ': 0.1012, 'NN': 0.012, 'RB': 0.0728, 'DT': 0.0479},
    'DT': {'NNP': 0.1147, 'MD': 0.0021, 'VB': 0.0002, 'JJ': 0.2157, 'NN': 0.4744, 'RB': 0.0102, 'DT': 0.0017}
}

tag_word_conditional = {
    'NNP': {'Janet': 0.000032, 'will': 0, 'back': 0, 'the': 0.000048, 'bill': 0},
    'MD': {'Janet': 0, 'will': 0.308431, 'back': 0, 'the': 0, 'bill': 0},
    'VB': {'Janet': 0, 'will': 0.000028, 'back': 0.000672, 'the': 0, 'bill': 0.000028},
    'JJ': {'Janet': 0, 'will': 0, 'back': 0.00034, 'the': 0, 'bill': 0},
    'NN': {'Janet': 0, 'will': 0.0002, 'back': 0.000223, 'the': 0, 'bill': 0.002337},
    'RB': {'Janet': 0, 'will': 0, 'back': 0.010446, 'the': 0, 'bill': 0},
    'DT': {'Janet': 0, 'will': 0, 'back': 0, 'the': 0.506099, 'bill': 0}
}

print('Janet will back the bill', most_likely_tagging('Janet will back the bill', tag_set, tag_tag_conditional, tag_word_conditional))
print('will Janet back the bill', most_likely_tagging('will Janet back the bill', tag_set, tag_tag_conditional, tag_word_conditional))
print('back the bill Janet will', most_likely_tagging('back the bill Janet will', tag_set, tag_tag_conditional, tag_word_conditional))