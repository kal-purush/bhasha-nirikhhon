from collections import deque

"""
deque 发音是 “deck”，”double-ended queue”的简称
"""


class LineHistory:
    def __init__(self, lines: object, histlen=3):
        self.lines = lines
        self.history = deque(maxlen=histlen)

    def __iter__(self):
        for lineno, line in enumerate(self.lines, 1):
            self.history.append((lineno, line))
            yield line

    def clear(self):
        self.history.clear()


if __name__ == '__main__':
    with open('/etc/passwd') as f:
        lines = LineHistory(f)
        for line in lines:
            if 'root' in line:
                for lineno, hline in lines.history:
                    print('{}:{}'.format(lineno, hline), end='')


"""
一个需要注意的小地方是，如果你在迭代操作时不使用for循环语句，那么你得先调用 iter() 函数
>>> f = open('somefile.txt')
>>> lines = linehistory(f)
>>> next(lines)
Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
TypeError: 'linehistory' object is not an iterator

>>> # Call iter() first, then start iterating
>>> it = iter(lines)
"""