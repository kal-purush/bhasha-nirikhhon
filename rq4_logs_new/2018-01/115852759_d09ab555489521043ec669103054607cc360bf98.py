"""
Investigating the most efficient (i.e. fastest) way of getting the status of many
(in the real world >> 10**6) URLs. Ordinary sequential request.head() is too slow.

So reads the status of a set of URLs listed in a file and then tries various means of
getting their statuses.
"""

import logging
import argparse
import time
import requests
import collections

def NaiveGet(urllist, **kwargs):
    """
    Does a naive get of the list of URLs by doing a simple loop which performs a request.head()
    call for each. This is the baseline for inefficiency
    :param urllist:
    :param kwargs:
    :return:
    """
    return {u: requests.head(u).status_code for u in urllist}

def FuckAll(urllist, **kwargs):
    """
    Does nothing. Just returns a list of 200 codes withouth checking any URLs
    :param urllist:
    :param kwargs:
    :return:
    """
    return {u: 200 for u in urllist}

class Test:
    """
    Results of the various IO latency tests
    """

    def __init__(self, urls, function, **kwargs):
        self.kwargs = kwargs
        self.size = len(urls)
        self.time = None
        self.function = function
        if self.size == 0:
            logging.error('Test {} has no URLs'.format(self.commentary))
        elif not callable(function):
            logging.error('Test {} has no callable function'.format(self.commentary))
        else:
            self.TimeExecution(urls)

    def __repr__(self):
        if self.time:
            kwstring = ", ".join(["{}={}".format(a, self.kwargs[a]) for a in self.kwargs])
            return "{} {:,} URLS in {:7.1g} sec = {:7.1g} per second".format(kwstring, self.size, self.time,
                                                                        self.size/self.time)
        else:
            return "Unknown test."

    def TimeExecution(self, urls):
        """
        Times execution of the function on the list of URLs
        :return:
        """
        t0 = time.time()
        r = self.function(urls)
        t1 = time.time()
        self.time = t1-t0
        self.counts = collections.Counter(r.values())

if __name__ == '__main__':

    ap = argparse.ArgumentParser(description='Time getting URLs')
    ap.add_argument('--urllist', metavar='file', help = 'URL list file', default='urllist.txt')
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)-7s %(message)s')

    t0 = time.time()
    try:
        with open(args.urllist) as urlfile:
            urllist = urlfile.read().splitlines()
    except (IOError, FileNotFoundError) as e:
        logging.critical("Error reading {}: {}".format(args.urllist, e))
    else:
        logging.debug("Read {} lines from {} in {:.1f} ms".format(len(urllist), args.urllist, (time.time()-t0)*1000))
        tests = [Test(urllist, f, commentary=c) for f,c in zip([FuckAll, NaiveGet],['No operation', 'Naive requests.get()'])]