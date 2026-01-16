class Logger(object):
    def __init__(self, spider_name):
        self.spider_name = spider_name.lower()
        self.spider_name = self.spider_name.replace(" ", "-")

    @staticmethod
    def log_it(text):
        filename = 'logs/reviews.log'
        with open(filename, 'a+') as f:
            f.write(text + "\n")
            print(text)