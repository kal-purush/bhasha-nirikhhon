from classes.RedditCrawler import RedditCrawler

class CrawlerFactory:
    @staticmethod
    def createRedditCrawler(keyword:str) ->RedditCrawler:
        crawler = RedditCrawler(keyword)
        return crawler