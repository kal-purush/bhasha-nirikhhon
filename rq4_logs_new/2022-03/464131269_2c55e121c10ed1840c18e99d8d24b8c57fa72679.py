from __future__ import annotations


class Interval:
    def __init__(self, lcp_value: int = 0, lb: int = 0, rb: int = -1, child_list=None):
        if child_list is None:
            child_list = []
        self.__lcp_value: int = lcp_value
        self.__lb: int = lb
        self.__rb: int = rb
        self.__child_list = child_list

    def get_lcp_value(self):
        return self.__lcp_value

    def get_lb(self):
        return self.__lb

    def get_rb(self):
        return self.__rb

    def get_child_list(self):
        return self.__child_list

    def update_child_list(self, interval):
        self.__child_list.append(interval)

    def set_lb(self, lb: int):
        self.__lb = lb

    def set_rb(self, rb: int):
        self.__rb = rb

    def set_lcp_value(self, lcp_value: int):
        self.__lcp_value = lcp_value


def add(top_interval: Interval, last_interval: Interval):
    top_interval.update_child_list(last_interval)


class SpecialStack:
    def __init__(self):
        self.__stack = []
        self.__len: int = 0

    def top(self):
        if self.__len <= 0:
            return None
        else:
            return self.__stack[self.__len - 1]

    def pop(self):
        self.__len -= 1
        return self.__stack.pop()

    def push(self, interval: Interval):
        self.__stack.append(interval)
        self.__len += 1

    def get_size(self):
        return self.__len

    def is_empty(self):
        return self.__len == 0


def process(lcp_interval: Interval):
    pass


def perform_bottom_up_traversal(s, suftab, lcp_table):
    last_interval: Interval | None = None
    stack: SpecialStack = SpecialStack()

    stack.push(Interval())

    for i in range(1, len(lcp_table)):
        lb = i - 1
        top: Interval = stack.top()

        while lcp_table[i] < top.get_lcp_value():
            top.set_rb(i - 1)
            last_interval: Interval = stack.pop()

            process(last_interval)

            lb: int = last_interval.get_lb()
            top = stack.top()

            if lcp_table[i] <= top.get_lcp_value():
                top.update_child_list(last_interval)
                last_interval = None

        if lcp_table[i] > top.get_lcp_value():
            if last_interval is not None:
                stack.push(Interval(lcp_table[i], lb, -1, [last_interval]))
                last_interval = None
            else:
                stack.push(Interval(lcp_table[i], lb, -1, []))
