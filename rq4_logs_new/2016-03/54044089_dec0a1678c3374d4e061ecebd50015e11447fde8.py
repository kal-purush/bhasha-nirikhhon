from common import Common
import pymysql.cursors
import pymysql

class Insert_db:

    def __init__(self):
        self.com = Common()

    def loadData(self):
        '''
        #usage: Load data form /home/boyang/Documents/furnitures
        #arg: None
        #return: Data stored at path above in json structure
        '''
        src = '/home/boyang/Documents/furnitures';
        datas = []
        furniture_list = self.com.GetFileList(src)
        for furniture in furniture_list:
            data = self.com.readJSON(furniture)
            datas.append(data)
        return datas

    def insertData(self, datas):
        '''
        #usage: Insert data into data base
        #arg: Data you want to insert
        #rerutn: None
        '''
        db = pymysql.connect(host = '127.0.0.1',
        					 user = 'root',
        					 password = 'Devil123',
        					 db = 'FurnitoData',
        					 charset = 'utf8',
        					 cursorclass = pymysql.cursors.DictCursor)
        cursor = db.cursor()
        for data in datas:
            comments = data['reviews']
            
            for comment in comments:
            	sql = 'INSERT INTO comments (furniture_name, comment, vote_up, vote_down) VALUES (%s, %s, %s, %s)'
            	print sql
                cursor.execute(sql, (data['name'].encode('utf-8'),comment.encode('utf-8'),'0','0'))
                db.commit()
        db.close()

i = Insert_db()
datas = i.loadData()
i.insertData(datas)