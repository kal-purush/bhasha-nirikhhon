file = "books/frankenstein.txt"

def gettext(sourcefile):
    with open(sourcefile) as f:
        file_contents = f.read()
    return file_contents

def getwords(sourcefile):
    with open(sourcefile) as f:
        file_contents  = f.read()
        wordcount = len(file_contents.split())
    return wordcount

def getletters(sourcefile):
    with open(sourcefile) as f:
        file_contents = f.read()
        words = file_contents.split()
        wordlist = {}
        for word in words:
            for letter in word:
                letter = letter.lower()
                if letter in wordlist:
                    wordlist[letter] += 1
                else:
                    wordlist[letter] = 1
        return wordlist

def sort_on(dict):
    return dict["value"]

def groupdicts(sourcefile):
    worddict = getletters(sourcefile)
    wordgroupdict = []
    for letter in worddict:
        wordgroupdict.append({"letter":letter, "value":worddict[letter]})
    wordgroupdict.sort(reverse=True, key=sort_on)
    return wordgroupdict


def getreport(sourcefile):
    sorteddict = groupdicts(sourcefile)
    print("--- Begin report of books/frankenstein.txt ---")
    print(f"{getwords(sourcefile)} words found in the document")
    print("")
    for entry in sorteddict:
        if str(entry["letter"]).isalpha():
            letter = entry["letter"]
            value = entry["value"]
            print(f"The '{letter}' character was found {value} times")
    print("--- End report ---")

if __name__ == "__main__":
    getreport(file)