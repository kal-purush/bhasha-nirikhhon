class Error(Exception):
    def __str__(self):
        return "error"

class RuntimeError(Error):
    def __str__(self):
        return "runtime error"

class DivideByZeroError(RuntimeError):
    def __str__(self):
        return ("runtime error: divide by zero")

class SyntaxError(Error):
    def __str__(self):
        return "syntax error"

class VariableAlreadyDefinedError(RuntimeError):
    def __str__(self):
        return ("runtime error: variable already defined")

class UndefinedVariableError(RuntimeError):
    def __str__(self):
        return ("runtime error: undefined variable")