'''
Created on 30-Jul-2016

@author: dgraja
'''

from elasticsearch import Elasticsearch


def main():
    es = Elasticsearch(["127.0.0.1:9200",])
    print es.cat.indices()
    return


if __name__ == '__main__':
    main()
    pass