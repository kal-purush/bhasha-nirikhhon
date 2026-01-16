import text
import difflib
import config

user = 'Kevin'


wordstoremove = ['help']


def locationOnly(strin):
    words = strin.split()
    for word in words:
        if len(difflib.get_close_matches(word, wordstoremove, 3 , .8)) > 0:
            strin = strin.replace(word, "")
    return strin


def sendHelp(location):
    location = locationOnly(location).strip()
    fromNumber = '+12622222437'
    body = user + " has listed you as his emergency contact. \nThey have signified they are in danger and need help."
    if location != "":
        body += "\nThey are located in " + location + "."
    for number in config.helpNumbers:
        text.send_text(body, number, fromNumber)