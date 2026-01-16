class DataMap:
    CSV = 1
    XML = 2
    TXT = 3
    JSON = 4

    def __init__(self, suffix, file=None, config={}):
        self.__suffix = suffix
        self.__file = file
        self.config = {}

    def convert2dict(self):
        ls = None
        if self.__suffix == DataMap.CSV or self.__suffix == DataMap.TXT:
            file = open(self.__file)
            for line in file:
                line = line.replace("\n", "")
                if line is None or line == "":
                    continue
                if self.__suffix == DataMap.CSV:
                    ls = line.split(",")
                if self.__suffix == DataMap.TXT:
                    ls = line.split("=")
                self.config[ls[0].strip()] = ls[1].strip()
        return self.config