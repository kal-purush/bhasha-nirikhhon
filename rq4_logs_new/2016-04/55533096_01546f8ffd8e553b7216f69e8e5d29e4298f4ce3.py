class Calc(object):

    display = ""
    curnumber = None
    numbers = None
    operations = None

    def __init__(self):
        self.operations = []
        self.numbers = []
        self.curnumber = ""

    def add_number(self):
        self.numbers.append(int(self.curnumber))
        self.curnumber = ""

    def clear(self):
        self.curnumber = ""
        self.operations = []
        self.numbers = []
        self.display = ""

    def press(self, symbol):
        if symbol.lower() == 'c':
            self.clear()
            return
        self.display += symbol
        if symbol.isdigit():
            self.curnumber += symbol
        elif symbol == '+':
            self.operations.append("+")
            self.add_number()
        elif symbol == '=':
            self.add_number()
            while len(self.operations) > 0:
                op = self.operations.pop()
                left, right = self.numbers.pop(), self.numbers.pop()
                if op == '+':
                    res = left + right
                    self.display = str(res)
                    self.curnumber = str(res)
                    self.numbers.append(res)