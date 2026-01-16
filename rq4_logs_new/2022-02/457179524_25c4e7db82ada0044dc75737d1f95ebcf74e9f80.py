def getSessionHeader(sessionName):
    splitstring = sessionName.split('-')

    monkey = splitstring[0]
    task = splitstring[1]
    date = splitstring[3]

    if date.endswith(('a','b')):
        date = date[0:len(date)-1]

    sessionheader = monkey+'-'+task+'-'+date
    return sessionheader