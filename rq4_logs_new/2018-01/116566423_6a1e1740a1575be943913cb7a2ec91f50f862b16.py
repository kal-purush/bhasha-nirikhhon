from .expression import Expression
from .money import Money

class Bank(object):
    """Bank class: temporary implementation
    """
    def __init__(self):
        self._rates = dict()

    def reduce(self, source: Expression, currency):
        return source.reduce(self, currency)

    def addRate(self, fromcurr, tocurr, rate):
        self._rates[(fromcurr, tocurr)] = rate

    def rate(self, fromcurr, tocurr):
        return self._rates.get((fromcurr, tocurr))