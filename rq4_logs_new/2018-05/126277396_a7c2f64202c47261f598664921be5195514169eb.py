from django.db import connection,transaction
import os
os.environ['DJANGO_SETTINGS_MODULE']='TISProduction.settings'
from TISProduction import tis_log

logger=tis_log.get_tis_logger()

def create_my_search():
    cursor=connection.cursor()
    cursor.execute('create vitual table my_search using FTS3 (table_name,obj_id,text)')
    transaction.commit_unless_managed()
    logger.debug(' created my_search virtual table')


def add_index(content):
    '''
    insert the index of FTS to my_search table
    :param content: a list of dict which contain {'table_name':, 'obj_id':,'text':}
    :return:
    '''
    cursor = connection.cursor()
    logger.debug(' startging save the index to my_search {0} records'.format(len(content)))
    for line in content:
        table_name=line.get('table_name')
        obj_id=line.get('obj_id')
        text=line.get('text')
        sql_str='insert into my_search values({0},{1},{2})'.format(table_name,obj_id,text)
        cursor.execute(sql_str)
    transaction.commit_unless_managed()
    logger.debug(' finished save the index to my_search {0} records'.format(len(content)))

def search_index(key_word):
    '''
    search the my_search table
    :param key_word: str - input by user
    :return: list of dict which contain {'table_name','obj_id'}
    '''
    table_l=[]
    cursor=connection.cursor()
    cursor.excute('select table_name,object_id from my_search where my_search match {0}'.format(key_word))
    result=cursor.fetchall() # result:[(table_name,obj_id),()]
    logger.debug(' find {} records in my_search for {1}'.format(len(result),key_word))
    for line in result:
        item={'table_name':line[0],'obj_id':line[1]}
        table_l.append(item)
    logger.debug(' assemble table info to list {0}'.format(table_l))
    return table_l
