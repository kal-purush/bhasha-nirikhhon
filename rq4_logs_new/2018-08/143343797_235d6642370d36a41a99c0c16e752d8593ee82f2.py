import requests
import threading
import time
import nltk
from bs4 import BeautifulSoup

SEARCH_TIME: float = 8.0

def get_links(query : str) -> list:
    page = requests.get("https://www.google.com/search?q="+query)
    soup = BeautifulSoup(page.content, features="html.parser")
    return [link["href"][7:] for link in soup.findAll("a") if link["href"][:7] == "/url?q="]

def get_links_test(query: str) -> str:
    page = requests.get("https://www.google.com/search?q="+query)
    soup = BeautifulSoup(page.content, features="html.parser")
    return soup.text
    

def get_mention_counts(text: str, phrases: list) -> str:
    all_valid_counts = []
    threads = []

    phrases_to_search : dict = valid_phrases(phrases)

    for phrase in phrases_to_search.keys():
        thread = threading.Thread(target=lambda text, phrase:\
            all_valid_counts.append((phrase, search_in_text(text, phrase))),
            args=(text, phrase))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    mention_counts = {phrase : 0 for phrase in phrases}
    for x, count in all_valid_counts:
        mention_counts[phrases_to_search[x]] += count
    return mention_counts.items()

def search_in_text(text: str, phrase: str) -> int:    
    return text.lower().count(phrase)

def valid_phrases(choices: list) -> list:
    valids = []
    for phrase in choices:
        valids.append((phrase, phrase))
        if len(phrase.split()) > 1:
            tokenized_phrase = nltk.pos_tag(nltk.word_tokenize(phrase))
            valids.extend([(token, phrase) for token, tag in tokenized_phrase
                             if tag[:2] == 'NN' and token != phrase])
    return dict(valids)

 
def evaluate_results(list_of_counts: list):
    #rough sketch
    #we need to choose one with most hits overall
    print(list_of_counts)
    answer = list_of_counts[0][0][0]
    confidence = 0.0

    phrase_counts = merge_counts(list_of_counts)

    total_count = sum([kv[1] for kv in phrase_counts])
    if total_count == 0: 
        return (answer, confidence)
    
    
    sorted_counts = sorted(phrase_counts, key=lambda kv: kv[1], reverse=True)
    current_confidence = float(sorted_counts[0][1]) / total_count
    for i in range(1, len(sorted_counts)):
        current_confidence -= float(sorted_counts[i][1]) / total_count
    if current_confidence > confidence:
        confidence = current_confidence
        answer = sorted_counts[0][0]

    return (answer, confidence)

def merge_counts(list_of_counts: list) -> list:
    first_counts = dict(list_of_counts[0])
    for counts in [list_of_counts[i] for i in range(1, len(list_of_counts))]:
        for phrase, count in counts:
            first_counts[phrase] += count
    return first_counts.items()

def run(question: str, choices: list, counts: list) -> str:
    text = get_links_test(question)
    counts.extend(get_mention_counts(text, choices))
    
def add_choices_to_question(question : str, choices: list) -> str:
    first_time = True
    for choice in choices:
        if first_time: 
            question += " AND " + choice
            first_time = False
        else:
            question += " OR " + choice 

        #deal with choice types that could be recognized under different circumstances
        if len(choice) > 0 and choice[-1] == 's': 
            question += " OR " + choice[:-1]
    return question

def clean_choices(choices: list):
    for i in range(len(choices)):
        choices[i] = choices[i].lower()
 
def answer(question : str, choices: list) -> str:
    start_time = time.time()    
    
    clean_choices(choices)

    threads = []
    counts_list = [[] for i in range(len(choices) + 2)]

    normal_thread = threading.Thread(target=(lambda q,c,cs: run(q,c,cs)), args=(question, choices, counts_list[-1]))
    all_choices_thread = threading.Thread(target=(lambda q,c,cs: run(q,c,cs)),\
                                       args=(add_choices_to_question(question, choices), choices, counts_list[-2]))
    threads.append(normal_thread)
    threads.append(all_choices_thread)
    for i in range(len(choices)):
        thread = threading.Thread(target=(lambda q,c,cs: run(q,c,cs)), 
            args=(add_choices_to_question(question, [choices[i]]), choices, counts_list[i]))
        threads.append(thread)

    
    for thread in threads: thread.start()
    for thread in threads: thread.join()


    threads = []    

    answer, confidence = evaluate_results(counts_list)

    print ("\n")
    print ("Web scraping results:")
    for i in range(len(counts_list)):
        print("\tIteration " + str(i+1) + ": " + str(counts_list[i]))
    print ("Confidence: " + str(confidence) + " for result of " + answer)
    print ("Time elapsed: " + str(time.time() - start_time))
    return answer