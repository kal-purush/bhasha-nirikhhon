class Closure:
    def __init__(self, params, body, environment):
        self.__params = params #params is a list of parameter names
        self.__body = body #body is a sequence parse
        self.__environment = environment

    def params(self):
        return self.__params;

    def body(self):
        return self.__body;

    def environment(self):
        return self.__environment;

    def __str__(self):
        return "(closure (parameters %s) %s)"%(' '.join(self.__params), self.__body)