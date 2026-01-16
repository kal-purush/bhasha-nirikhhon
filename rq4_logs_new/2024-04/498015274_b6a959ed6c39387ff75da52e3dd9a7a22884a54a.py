#!/usr/bin/env python3
class TimeoutError(StopIteration):
   pass

class TaskNotFoundError(IndexError):
    pass
        