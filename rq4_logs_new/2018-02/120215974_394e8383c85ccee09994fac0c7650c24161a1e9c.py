from textblob import TextBlob
from nltk.corpus import cmudict
import random
import sys
import re


class InputError(Exception):
    pass


def define_word(word):
    common_words = ["a", "an", "as", "and", "or", "is", "the", "in", "s", "so", "ve", "'", "m"]
    special_chars = ",.:;\"|\!@#$%^&*()_+-=[]{}<>?/~`’'”'"
    stripped = word.strip(special_chars).lower()
    if not (stripped in common_words) or (len(stripped) == 0):
        return stripped
    return ""


def syllables(word):
    word = word.lower()
    # exception_add are words that need extra syllables
    # exception_del are words that need less syllables
    exception_add = ['serious', 'crucial']
    exception_del = ['fortunately', 'unfortunately']
    co_one = ['cool', 'coach', 'coat', 'coal', 'count', 'coin', 'coarse', 'coup', 'coif', 'cook', 'coign', 'coiffe',
                  'coof', 'court']
    co_two = ['coapt', 'coed', 'coinci']
    pre_one = ['preach']
    syls = 0  # added syllable number
    disc = 0  # discarded syllable number
    # 1) if letters < 3 : return 1
    if len(word) <= 3:
        syls = 1
        return syls
    # 2) if doesn't end with "ted" or "tes" or "ses" or "ied" or "ies", discard "es" and "ed" at the end.
    # if it has only 1 vowel or 1 set of consecutive vowels, discard. (like "speed", "fled" etc.)
    if word[-2:] == "es" or word[-2:] == "ed":
        doubleAndtripple_1 = len(re.findall(r'[eaoui][eaoui]', word))
        if doubleAndtripple_1 > 1 or len(re.findall(r'[eaoui][^eaoui]', word)) > 1:
            if word[-3:] == "ted" or word[-3:] == "tes" or word[-3:] == "ses" or word[-3:] == "ied" or \
                                word[-3:] == "ies":
                pass
            else:
                disc += 1
    # 3) discard trailing "e", except where ending is "le"
    le_except = ['whole', 'mobile', 'pole', 'male', 'female', 'hale', 'pale', 'tale', 'sale', 'aisle', 'whale',
                     'while']
    if word[-1:] == "e":
        if word[-2:] == "le" and word not in le_except:
            pass
        else:
            disc += 1
    # 4) check if consecutive vowels exists, triplets or pairs, count them as one.
    doubleAndtripple = len(re.findall(r'[eaoui][eaoui]', word))
    tripple = len(re.findall(r'[eaoui][eaoui][eaoui]', word))
    disc += doubleAndtripple + tripple
    # 5) count remaining vowels in word.
    numVowels = len(re.findall(r'[eaoui]', word))
    # 6) add one if starts with "mc"
    if word[:2] == "mc":
        syls += 1
    # 7) add one if ends with "y" but is not surrouned by vowel
    if word[-1:] == "y" and word[-2] not in "aeoui":
        syls += 1
    # 8) add one if "y" is surrounded by non-vowels and is not in the last word.
    for i, j in enumerate(word):
        if j == "y":
            if (i != 0) and (i != len(word) - 1):
                if word[i - 1] not in "aeoui" and word[i + 1] not in "aeoui":
                    syls += 1
    # 9) if starts with "tri-" or "bi-" and is followed by a vowel, add one.
    if word[:3] == "tri" and word[3] in "aeoui":
        syls += 1
    if word[:2] == "bi" and word[2] in "aeoui":
        syls += 1
    # 10) if ends with "-ian", should be counted as two syllables, except for "-tian" and "-cian"
    if word[-3:] == "ian":
        # and (word[-4:] != "cian" or word[-4:] != "tian") :
        if word[-4:] == "cian" or word[-4:] == "tian":
            pass
        else:
            syls += 1
    # 11) if starts with "co-" and is followed by a vowel, check if exists in the double syllable dictionary,
    # if not, check if in single dictionary and act accordingly.
    if word[:2] == "co" and word[2] in 'eaoui':
        if word[:4] in co_two or word[:5] in co_two or word[:6] in co_two:
            syls += 1
        elif word[:4] in co_one or word[:5] in co_one or word[:6] in co_one:
            pass
        else:
            syls += 1
    # 12) if starts with "pre-" and is followed by a vowel,
    # check if exists in the double syllable dictionary, if not, check if in single dictionary and act accordingly.
    if word[:3] == "pre" and word[3] in 'eaoui':
        if word[:6] in pre_one:
            pass
        else:
            syls += 1
    # 13) check for "-n't" and cross match with dictionary to add syllable.
    negative = ["doesn't", "isn't", "shouldn't", "couldn't", "wouldn't"]
    if word[-3:] == "n't":
        if word in negative:
            syls += 1
        else:
            pass
    # 14) Handling the exceptional words
    if word in exception_del:
        disc += 1
    if word in exception_add:
        syls += 1
    # calculate the output
    return numVowels - disc + syls


def nsyl(word):
    return syllables(word)


def make_text_line_1(filename):
    wl = ""
    with open(filename) as file:
        all_words = file.read()
        blob = TextBlob(all_words)
        word_list = blob.words
        line_1 = ""
        for word in word_list:
            stripped = define_word(word)
            if stripped != "":
                wl += stripped + " "
            else:
                continue
        for word in random.sample(wl.split(), 5):
            try:
                if nsyl(word) == 5:
                    line_1 += word + " "
                    break
                elif nsyl(word) == 4:
                    line_1 += word + " "
                    if nsyl("".join(line_1.split())) == 5:
                        break
                elif nsyl(word) == 3:
                    line_1 += word + " "
                    if nsyl("".join(line_1.split())) == 5:
                        break
                elif nsyl(word) == 2:
                    line_1 += word + " "
                    if nsyl("".join(line_1.split())) == 5:
                        break
                elif nsyl(word) == 1:
                    line_1 += word + " "
                    if nsyl("".join(line_1.split())) == 5:
                        break
            except:
                continue
        return line_1


def make_text_line_2(filename):
    wl = ""
    with open(filename) as file:
        all_words = file.read()
        blob = TextBlob(all_words)
        word_list = blob.words
        line_2 = ""
        for word in word_list:
            stripped = define_word(word)
            if stripped != "":
                wl += stripped + " "
            else:
                continue
        for word in random.sample(wl.split(), 6):
            try:
                if nsyl(word) == 7:
                    line_2 += word + " "
                    break
                elif nsyl(word) == 6:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
                elif nsyl(word) == 5:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
                elif nsyl(word) == 4:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
                elif nsyl(word) == 3:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
                elif nsyl(word) == 2:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
                elif nsyl(word) == 1:
                    line_2 += word + " "
                    if nsyl("".join(line_2.split())) == 7:
                        break
            except:
                continue
        return line_2


def make_text_line_3(filename):
    wl = ""
    with open(filename) as file:
        all_words = file.read()
        blob = TextBlob(all_words)
        word_list = blob.words
        line_3 = ""
        for word in word_list:
            stripped = define_word(word)
            if stripped != "":
                wl += stripped + " "
            else:
                continue
        for word in random.sample(wl.split(), 5):
            try:
                if nsyl(word) == 5:
                    line_3 += word + " "
                    break
                elif nsyl(word) == 4:
                    line_3 += word + " "
                    if nsyl("".join(line_3.split())) == 5:
                        break
                elif nsyl(word) == 3:
                    line_3 += word + " "
                    if nsyl("".join(line_3.split())) == 5:
                        break
                elif nsyl(word) == 2:
                    line_3 += word + " "
                    if nsyl("".join(line_3.split())) == 5:
                        break
                elif nsyl(word) == 1:
                    line_3 += word + " "
                    if nsyl("".join(line_3.split())) == 5:
                        break
            except:
                continue
        return line_3


def main():
    haiku = []
    while True:
        try:
            name = "your_name"
            filename = "eecummingspoem.txt"
            # if "txt" not in filename.split(".") and "htm" not in filename.split(".") \
                    #and "html" not in filename.split("."):
                # raise InputError
            haiku.append(make_text_line_1(filename))
            haiku.append(make_text_line_2(filename))
            haiku.append(make_text_line_3(filename))
            rendered_haiku = TextBlob("\n".join(haiku))
            print("\n".join(haiku))
            print("-", name, sep="", end="\n\n")
            print("Your haiku is", end="")
            if rendered_haiku.sentiment.polarity < 0:
                print(" {0:.2f}".format(100 * rendered_haiku.sentiment.polarity), "% negative.", sep="")
            elif rendered_haiku.sentiment.polarity > 0:
                print(" {0:.2f}".format(100 * rendered_haiku.sentiment.polarity), "% positive.", sep="")
            else:
                print(" neutral and its sentiment is 0%.")

            break
        # except InputError or ValueError:
            # print("Please enter a valid file and/or name.")


main()