from pyspark import SparkContext,SparkConf


class JobContext:

    __broadcast = {}
    __sc = None

    @classmethod
    def getsparkcontext(cls):
        if not cls.__sc == None:
            return cls.__sc

        __conf = SparkConf().setMaster("local").setAppName("Football-Analysis")
        cls.__sc = SparkContext().getOrCreate(__conf)
        return cls.__sc

    @classmethod
    def setBroadCast(cls,key,value):
        cls.__broadcast[key] = value

    @classmethod
    def getBoadCast(cls,key):
        return cls.__broadcast[key]

    def test(self):
        pass