# decompyle3 version 3.3.2
# Python bytecode 3.7 (3394)
# Decompiled from: Python 3.7.6 (default, Dec 30 2019, 19:38:26) 
# [Clang 11.0.0 (clang-1100.0.33.16)]
# Embedded file name: mittarikone.py
# Size of source mod 2**32: 7020 bytes
from multiprocessing import Process, Lock, Queue

def stripper(data, mode=0):
    """1 text string; strips everything besides letters, whitespace and "." (mode 0)
    + numbers extra characters (mode 1) to eliminate extra characters. Output string"""
    chars = ''
    if mode == 1:
        chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖabcdefghijklmnopqrstuvwxyzåäö0123456789.,;:#@ '
    elif mode == 0:
        chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖabcdefghijklmnopqrstuvwxyzåäö.#@ '
    temp = ''.join(list(filter(lambda x: x in chars, data)))
    return temp


def strip_html(data):
    """1 text string; strips html <elements>, output string"""
    from html.parser import HTMLParser

    class MyParser(HTMLParser):
        content = ''
        passthis = 0

        def handle_starttag(self, tag, attrs):
            if tag == 'script':
                self.passthis = 1
            if tag == 'style':
                self.passthis = 1

        def handle_data(self, data):
            if self.passthis == 0:
                self.content = self.content + str(data)
            else:
                self.passthis = 0

        def get_content(self):
            return self.content

    parser = MyParser()
    temp = parser.feed(data)
    temp = parser.get_content()
    return temp


def strip_dic(data):
    """1 text string; strips common Finnish words from the string.
        Output string"""
    try:
        file = open('dict_FIN.txt', 'r', encoding='utf-8')
    except Exception:
        print('Dictionary file missing!')
        raise

    wordlist = file.readlines()
    file.close()
    for i in range(0, len(wordlist)):
        wordlist[i] = wordlist[i].split(' ')
        wordlist[i] = wordlist[i][1][:-1]

    temppi = data.split(' ')
    finaltext = ''
    for i in range(0, len(temppi)):
        if len(temppi[i]) < 2:
            continue
        if temppi[i].upper() not in wordlist:
            finaltext = finaltext + str(temppi[i]) + ' '

    return finaltext


def get_html(url):
    """1 http address as string. Returns the page content as string.
    Artificial delay and other tactics to circumvent bot-catchers."""
    import urllib.request, time, random
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    time.sleep(random.randint(0, 5))
    page = opener.open(url)
    content = page.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin1')

    return text


def get_meta(place=''):
    """Returns metadata from open data. Returns list. Additional
    attribute place given as string for more precise location, eg. "Kotka" """
    pass


def similar_sort(text, mode):
    """Sorts words into list of similar words. Mode 1=words, 2 = sentences.
    File "" no start file, file <name> start from output."""
    import difflib
    if mode == 1:
        lista = text.split(' ')
        cpoint = 0.75
    elif mode == 2:
        lista = text.split('.')
        cpoint = 0.7
    hakusana = []
    maara = []
    tulos = []
    osalista = []
    paikka = 0
    templista = []
    retest = []
    for i in range(0, len(lista)):
        osalista.append(lista[i].upper())

    for i in range(0, len(osalista)):
        retest = []
        retest = difflib.get_close_matches((osalista[i]), hakusana, n=1, cutoff=cpoint)
        if osalista[i] in hakusana:
            paikka = hakusana.index(osalista[i])
            maara[paikka] = maara[paikka] + 1
        else:
            if retest != []:
                paikka = hakusana.index(retest[0])
                maara[paikka] = maara[paikka] + 1
            else:
                hakusana.append(osalista[i])
                maara.append(1)

    for i in range(0, len(hakusana)):
        tulos.append([hakusana[i], str(maara[i])])

    print('Analysis complete, creating log.')
    return tulos


def read_log(nimi):
    """reads a log written by write_log and parses it into a list."""
    try:
        file = open(nimi, 'r', encoding='latin1')
        content = file.read()
        return content
    except Exception:
        print('Cannot read from temp file!')
        raise


def calculate_log(lista, nimi='', meta=''):
    """Writes list lista into a file; name given, or
    "hour-day-month-year-output.log" """
    import time
    if nimi == '':
        nimi = str(time.strftime('%H-%d-%m-%Y', time.gmtime())) + '-' + meta + '-output.csv'
    outputfile = open('./outputs/' + nimi, 'w')
    templine = ''
    for i in range(0, len(lista)):
        templine = str(lista[i][0]) + ';' + str(lista[i][1] + '\n')
        outputfile.write(templine)

    outputfile.close()
    print('Log ' + nimi + ' written.')


def put_log_data(dataset, lock, flush=0):
    """Used to save temporary log for data. Call calculate_log to create report"""
    if flush == 1:
        file = open('put_log_file.txt', 'w')
        file.close()
        return 0
    try:
        try:
            lock.acquire()
            file = open('put_log_file.txt', 'a')
        except Exception:
            print('Cannot write to temp file!')
            raise

    finally:
        lock.release()

    file.write(dataset)
    file.close()


def get_data_set(url, lock):
    print('Working on: ', url)
    data = get_html(url)
    data = strip_html(data)
    data = stripper(data)
    data = strip_dic(data)
    put_log_data(data, lock, flush=0)


def main():
    lock = Lock()
    put_log_data('asdasdasda', lock, flush=1)
    try:
        sourcefile = open('sources.txt', 'r')
        sourcelist = sourcefile.readlines()
    except IOError:
        print('Source file broken.')
        raise

    process_set = []
    for i in range(0, len(sourcelist)):
        process_set.append(Process(target=get_data_set, args=(sourcelist[i][:-1], lock)))
        print('Started: ' + sourcelist[i][:-1])

    for i in range(0, len(process_set)):
        process_set[i].start()

    for i in range(0, len(process_set)):
        process_set[i].join()

    print('Data collected, analyzing.')
    dataset = read_log('put_log_file.txt')
    list_data = similar_sort(dataset, mode=1)
    calculate_log(list_data, meta='words')
    list_data = similar_sort(dataset, mode=2)
    calculate_log(list_data, meta='sentences')
    print('All done!')


if __name__ == '__main__':
    main()
# okay decompiling crawler.cpython-37.pyc