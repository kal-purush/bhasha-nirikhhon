import os.path, math
import sys
import re
import random

def removeStopwords(wordlist):
    stopwords = ['a', 'about', 'above', 'across', 'after', 'afterwards']
    stopwords += ['again', 'against', 'all', 'almost', 'alone', 'along']
    stopwords += ['already', 'also', 'although', 'always', 'am', 'among']
    stopwords += ['amongst', 'amoungst', 'amount', 'an', 'and', 'another']
    stopwords += ['any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere']
    stopwords += ['are', 'around', 'as', 'at', 'back', 'be', 'became']
    stopwords += ['because', 'become', 'becomes', 'becoming', 'been']
    stopwords += ['before', 'beforehand', 'behind', 'being', 'below']
    stopwords += ['beside', 'besides', 'between', 'beyond', 'bill', 'both']
    stopwords += ['bottom', 'but', 'by', 'call', 'can', 'cannot', 'cant']
    stopwords += ['co', 'computer', 'con', 'could', 'couldnt', 'cry', 'de']
    stopwords += ['describe', 'detail', 'did', 'do', 'done', 'down', 'due']
    stopwords += ['during', 'each', 'eg', 'eight', 'either', 'eleven', 'else']
    stopwords += ['elsewhere', 'empty', 'enough', 'etc', 'even', 'ever']
    stopwords += ['every', 'everyone', 'everything', 'everywhere', 'except']
    stopwords += ['few', 'fifteen', 'fifty', 'fill', 'find', 'fire', 'first']
    stopwords += ['five', 'for', 'former', 'formerly', 'forty', 'found']
    stopwords += ['four', 'from', 'front', 'full', 'further', 'get', 'give']
    stopwords += ['go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her']
    stopwords += ['here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers']
    stopwords += ['herself', 'him', 'himself', 'his', 'how', 'however']
    stopwords += ['hundred', 'i', 'ie', 'if', 'in', 'inc', 'indeed']
    stopwords += ['interest', 'into', 'is', 'it', 'its', 'itself', 'keep']
    stopwords += ['last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made']
    stopwords += ['many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine']
    stopwords += ['more', 'moreover', 'most', 'mostly', 'move', 'much']
    stopwords += ['must', 'my', 'myself', 'name', 'namely', 'neither', 'never']
    stopwords += ['nevertheless', 'next', 'nine', 'no', 'nobody', 'none']
    stopwords += ['noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of']
    stopwords += ['off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or']
    stopwords += ['other', 'others', 'otherwise', 'our', 'ours', 'ourselves']
    stopwords += ['out', 'over', 'own', 'part', 'per', 'perhaps', 'please']
    stopwords += ['put', 'rather', 're', 's', 'same', 'see', 'seem', 'seemed']
    stopwords += ['seeming', 'seems', 'serious', 'several', 'she', 'should']
    stopwords += ['show', 'side', 'since', 'sincere', 'six', 'sixty', 'so']
    stopwords += ['some', 'somehow', 'someone', 'something', 'sometime']
    stopwords += ['sometimes', 'somewhere', 'still', 'such', 'system', 'take']
    stopwords += ['ten', 'than', 'that', 'the', 'their', 'them', 'themselves']
    stopwords += ['then', 'thence', 'there', 'thereafter', 'thereby']
    stopwords += ['therefore', 'therein', 'thereupon', 'these', 'they']
    stopwords += ['thick', 'thin', 'third', 'this', 'those', 'though', 'three']
    stopwords += ['three', 'through', 'throughout', 'thru', 'thus', 'to']
    stopwords += ['together', 'too', 'top', 'toward', 'towards', 'twelve']
    stopwords += ['twenty', 'two', 'un', 'under', 'until', 'up', 'upon']
    stopwords += ['us', 'very', 'via', 'was', 'we', 'well', 'were', 'what']
    stopwords += ['whatever', 'when', 'whence', 'whenever', 'where']
    stopwords += ['whereafter', 'whereas', 'whereby', 'wherein', 'whereupon']
    stopwords += ['wherever', 'whether', 'which', 'while', 'whither', 'who']
    stopwords += ['whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with']
    stopwords += ['within', 'without', 'would', 'yet', 'you', 'your']
    stopwords += ['yours', 'yourself', 'yourselves']

    return [w for w in wordlist if w not in stopwords]

def test_read(filename):
    with open(filename, encoding="utf8") as f:
      while True:
        c = f.read(1).encode("utf-8")
        if not c:
          print("End of file")
          break
        print(c)


def read_file(fileName):
    file = open(fileName, encoding='utf-8')
    text = file.read()
    file.close()
    return text

def clean_up(s):
    ''' Return a version of string str in which all letters have been
    converted to lowercase and punctuation characters have been stripped
    from both ends. Inner punctuation is left untouched. '''

    punctuation = '''!"',;:.-?)([]<>*#\n\t\r'''
    result = s.lower().strip(punctuation)
    return result


def word_type_count(text):

    path = 'D:\CSE\Python\Words'

    #nouns = read_file(path + "\Nouns\91K_nouns.txt")
    verbs = read_file(path + r'\Verbs\31K_verbs.txt')
    adverbs = read_file(path + r'\Adverbs\6K_adverbs.txt')
    adjectives = read_file(r'D:\CSE\Python\Words\Adjectives\28K_adjectives.txt')

    noun_count = 0
    verb_count = 0
    adverb_count = 0
    adjective_count = 0

    for word in text.split(' '):
        regex = 'r\'\b(' + clean_up(word) + ')\b'
        if re.search(regex, nouns):
            noun_count += 1
        if re.search(regex, verbs):
            verb_count += 1
        elif re.search(regex, adverbs):
            adverb_count += 1
        elif re.search(regex, adjectives):
            adjective_count += 1

    word_types = [noun_count, verb_count, adverb_count, adjective_count]
    return word_types


def split_sentence(text):

    periodSplit = text.split('.')
    finalSplit = []

    for period in periodSplit:
        exclaimSplit = period.split('!')
        for exclaim in exclaimSplit:
            questionSplit = exclaim.split('?')
            for question in questionSplit:
                if (len(question) > 0):
                    finalSplit.append(question)
    return finalSplit


def split_phrase(text):

    commaSplit = text.split(',')
    finalSplit = []

    for comma in commaSplit:
        semiSplit = comma.split(';')
        for semi in semiSplit:
            hyphenSplit = semi.split('-')
            for hyphen in hyphenSplit:
                if len(hyphen) > 0:
                    finalSplit.append(hyphen)

    return finalSplit


def attribute_text(text):

    text = remove_action(text)

    lines = str.splitlines(text)

    dialogue = {  # This is as close as I'm gonna get to type safety here
        'Alistair': '',
        'Anders': '',
        'Aveline': '',
        'Bethany': '',
        'Blackwall': '',
        'Carver': '',
        'Cassandra': '',
        'Cole': '',
        'Dorian': '',
        'Fenris': '',
        'Iron Bull': '',
        'Isabela': '',
        'Leliana': '',
        'Loghain': '',
        'Merrill': '',
        'Morrigan': '',
        'Oghren': '',
        'Sebastian': '',
        'Sera': '',
        'Shale': '',
        'Solas': '',
        'Sten': '',
        'Varric': '',
        'Vivienne': '',
        'Wynne': '',
    }

    speech = ''

    for line in lines:
        if len(line) > 0 and line[0] != '─':
            line = str.strip(line).split(':')
            speaker = line[0]
            if len(line) > 1:
                speech = line[1]
            if speaker in dialogue:
                dialogue[speaker] = '%s %s' % (dialogue[speaker], speech)

    return dialogue


def remove_action(text):
    return re.sub('\((.*?)\)', '', text)


def avg_word_length(text):

    words = text.split(' ')
    totalWords = -1
    length = 0

    for word in words:
        word = clean_up(word)
        length += len(word)
        totalWords += 1

    if totalWords > 0:
        return length/totalWords
    else:
        return -1


def type_token_ratio(text):

    words = text.split(' ')
    distinctWords = []

    for word in words:
        word = clean_up(word)
        if word not in distinctWords:
            distinctWords.append(word)

    if len(words) > 0:
        return len(distinctWords)/len(words)
    else:
        return -1


def hapax_legomena_ratio(text):

    words = text.split(' ')
    allWords = []
    uniqueWords = []

    for word in words:
        word = clean_up(word)
        if word not in allWords:
            uniqueWords.append(word)
        elif word in uniqueWords:
            uniqueWords.remove(word)
        allWords.append(word)

    if len(words) > 0:
        return len(uniqueWords)/len(words)
    else:
        return -1


def avg_sentence_length(text):

    sentences = len(split_sentence(text))

    if sentences > 0:
        return len(text.split(' '))/sentences
    else:
        return -1


def avg_sentence_complexity(text):

    sentences = len(split_sentence(text))

    if sentences > 0:
        return len(split_phrase(text))/len(split_sentence(text))
    else:
        return -1


def filler_ratio(text):
    text = text.lower()
    filler = re.findall('( um | um,| um\.\.\.| er | er,| er\.\.\.| ah | ah,| ah\.\.\.| uh | uh,| uh\.\.\.)', text)

    words = len(text.split(' '))

    if words > 0:
        return len(filler)/len(text.split(' '))
    else:
        return -1


def obscenity_usage(text):

    obscenities = re.findall('( ass | ass\.|asshole|arse|bastard|bitch|cunt|damn|dick|douche|fuck|hell|piss|pussy|shit|slut|whore)', text)
    return len(obscenities)


def fuck_count(text):
    return len(re.findall('fuck', text))


def contraction_ratio(text):

    full = ['do not', 'what is', 'where is', 'where did', 'would not', 'could not', 'cannot', 'can not', 'will not', 'are not', 'is not', 'have not', 'had not', 'has not', 'he is', 'he will', 'she is', 'she will', 'they are', 'they will', 'I am', 'should have']
    short = ['don\'t', 'what\'s', 'where\'s', 'where\'d', 'wouldn\'t', 'couldn\'t', 'can\'t', 'won\'t', 'aren\'t', 'isn\'t', 'haven\'t', 'hasn\'t', 'hadn\'t', 'he\'s', 'he\'ll', 'she\'s', 'she\'ll', 'they\'re', 'they\'ll', 'I\'m', 'should\'ve']

    fullPhrases = 0
    contractions = 0

    for fullPhrase in full:
        regex = fullPhrase
        fullPhrases += len(re.findall(regex, text))

    for contraction in short:
        regex = contraction
        contractions += len(re.findall(regex, text))

    if fullPhrases > 0:
        return contractions/fullPhrases
    else:
        return -1


def religion_ratio(text):

    words = text.split(' ')
    religiousWords = re.findall('(Andraste|Maker|Chant|Divine|Holy)', text)

    numWords = len(words)

    if numWords > 0:
        return len(religiousWords)/numWords
    else:
        return -1


def elf_ratio(text):

    words = text.split(' ')
    religiousWords = re.findall(r'\b(Fen\'Harel | Elgar\'nan | Mythal | Falon\'Din | Dirthamen | Sylaise | Andruil | June | Ghilan\'nain | Arlathan)\b', text)

    numWords = len(words)

    if numWords > 0:
        return len(religiousWords)/numWords
    else:
        return -1


def compare_signatures(sig1, sig2, weight):

    difference = 0
    for x in range (0, 9):
        difference += abs(sig1[x] - sig2[x]) * weight[x]

    return difference


def find_match (attributes, resultSet):

    weight = [11, 33, 50, 0.4, 4, 2, 2, .1, .1, .1]
    min = compare_signatures(attributes, resultSet['Blackwall'], weight)
    match = 'Blackwall'

    for char in resultSet:
        if resultSet[char]:
            diff = compare_signatures(attributes, resultSet[char], weight)
            if diff < min:
                min = diff
                match = char

    return match


def randomize_weights():

    weights = []

    for x in range (0,9):
        w = random.random()
        weights.append(w)

    return weights


def print_result(resultSet):  # If you get -1 something's fucked

    for i in range (0, len(resultSet)):
        if i == 7:
            print('%.0f' %(resultSet[i]))
        else:
            print('%.4f' %(resultSet[i]))

    print('\n')


def get_attributes(charSpeak):

    attributes = []
    wordLength = avg_word_length(charSpeak)
    attributes.insert(0, wordLength)

    ttr = type_token_ratio(charSpeak)
    attributes.insert(1, ttr)

    hlr = hapax_legomena_ratio(charSpeak)
    attributes.insert(2, hlr)

    sentenceLength = avg_sentence_length(charSpeak)
    attributes.insert(3, sentenceLength)

    complexity = avg_sentence_complexity(charSpeak)
    attributes.insert(4, complexity)

    contractions = contraction_ratio(charSpeak)
    attributes.insert(5, contractions)

    filler = filler_ratio(charSpeak)
    attributes.insert(6, filler)

    obscenity = obscenity_usage(charSpeak)
    attributes.insert(7, obscenity)

    religiousness = religion_ratio(charSpeak)
    attributes.insert(8, religiousness)

    elfiness = elf_ratio(charSpeak)
    attributes.insert(9, elfiness)

    return attributes


def print_all (resultSet):
    for char in resultSet.keys():
        print(char)
        if resultSet[char]:
            print_result(resultSet[char])


def find_author(control, test):
    test = get_attributes(remove_action(test))
    resultSet = get_result_set(control)


    return find_match(test, resultSet)

def get_result_set(control):
    dialogue = attribute_text(remove_action(control))

    resultSet = {'': []}

    for character in dialogue.keys():
        attributes = get_attributes(dialogue[character])
        resultSet[character] = attributes

    return resultSet

def print_character(character):

    file = read_file(sys.argv[1])
    dialogue = attribute_text(remove_action(file))

    print(dialogue[character])

def main():

    path = 'D:\CSE\Python\Test_Files'
    filename = "\\varric_test_2.txt"
    test = read_file(path+filename)

    control = read_file(path+"\dai_readable.txt")

    print(word_type_count(test))
    print_all(get_result_set(control))
    print(find_author(control, test))

    return(0)

if __name__ == '__main__':
    main()