#!/usr/bin/python3
apiKey      = ''
blacklist   = ''
historyFile = '~/.queryHistory'

import os
import sys
import atexit
import readline
import urllib.request
import urllib.parse
import json
try:
    from colorama import init, Fore, Style
except ImportError:
    print(Fore.RED + Style.BRIGHT + 'Colorama is not installed.' + Style.RESET_ALL)
    sys.exit()

# Read query history and save it on exit.
queryHistory = os.path.expanduser(historyFile)
open(queryHistory, 'a').close()
readline.read_history_file(queryHistory)
atexit.register(readline.write_history_file, queryHistory)

# Initialize Colorama.
init()

# Ensure an API key was entered into the above variable.
if not apiKey:
    print(Fore.RED + Style.BRIGHT + 'No API key provided.' + Style.RESET_ALL)
    sys.exit()


def printResult(query):
    # Make a request to WolframAlpha's API using the provided key.
    # urllib.parse.quote converts the query into a 'URL-friendly' format (e.g. %20 for space).
    webResponse = urllib.request.urlopen('https://api.wolframalpha.com/v2/query?input=' + urllib.parse.quote(query)
                                         + '&format=plaintext&output=JSON&appid=' + apiKey)

    # Convert the JSON data we just received into UTF-8.
    strResponse = webResponse.read().decode('utf-8')

    # Deserialize the JSON data into a workable Python object.
    apiResponse = json.loads(strResponse)

    # If the query was not successful, quit.
    # Otherwise, pick out the relevant details we want to display.
    if apiResponse['queryresult']['success'] == False:
        print(Fore.RED + Style.BRIGHT + 'Unrecognized query.' + Style.RESET_ALL)
    else:
        # Loop through the JSON response...
        # If the current section isn't in the blacklist and its contents aren't empty, print it.
        for section in apiResponse['queryresult']['pods']:
            title = section['title']
            text  = section['subpods'][0]['plaintext']
            if title not in blacklist and text:
                print(Fore.BLUE + Style.BRIGHT + title + ':' + Fore.RESET + Style.RESET_ALL, end=' ')
                if '\n' in text: print()
                print(text)


# Prompt user for query.
def askQuery():
    global query
    while True:
        print(Fore.BLUE + 'Query:' + Fore.RESET)
        query = input().strip()
        if query: break


try:
    if len(sys.argv) > 1:
        # Check command-line arguments for continuous mode.
        if sys.argv[1] == '-c':
            while True:
                askQuery()
                printResult(query)
        # Otherwise, interpret them as a query and merge everything into one string.
        else:
            query = ' '.join(sys.argv[1:])
            printResult(query)
    # No command-line arguments or query provided, so prompt user.
    else:
        askQuery()
        printResult(query)
# Suppress keyboard interrupt (Ctrl-C) traceback for clean exit.
except KeyboardInterrupt:
    print()
    sys.exit()