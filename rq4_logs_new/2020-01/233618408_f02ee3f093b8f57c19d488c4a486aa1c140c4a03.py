class Game(object):
    def __init__(self, num):
        self.raw_num = num

    def number_off(self):
        result = ""
        if self.raw_num % 3 == 0:
            result += 'Fizz'
        if self.raw_num % 5 == 0:
            result += 'Buzz'
        return str(self.raw_num) if result == "" else result