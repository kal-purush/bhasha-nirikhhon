import pymysql
from scrapy.utils.project import get_project_settings


class DBHelper(object):

    def __init__(self):
        self.settings = get_project_settings()  # 获取settings配置数据

        self.host = self.settings['MYSQL_HOST']
        self.port = self.settings['MYSQL_PORT']
        self.user = self.settings['MYSQL_USER']
        self.passwd = self.settings['MYSQL_PASSWD']
        self.db = self.settings['MYSQL_DBNAME']
        self.port = self.settings['MYSQL_PORT']

    def conn(self):
        # print(f"被调用")
        _conn = pymysql.connect(host=self.host,
                                user=self.user,
                                password=self.passwd,
                                database=self.db,
                                port=self.port,
                                charset='utf8mb4'
                                )
        return _conn

    def insert(self, sql, _data=None):
        try:
            conn = self.conn()
            # print(f"数据库状态", conn.ping())
            cursor = conn.cursor()
            conn.ping(reconnect=True)
            cursor.execute(sql, _data)
            return cursor.lastrowid
        except Exception as e:
            print(f"出错了", e)
            print(str(e))
            conn.rollback()
        finally:
            conn.commit()
            cursor.close()

    def exit_id(self, sql, _data=None):
        try:
            conn = self.conn()
            print(f"查询数据库状态", conn.ping())
            with conn:
                with conn.cursor() as cursor:
                    conn.ping(reconnect=True)
                    cursor.execute(sql, _data)
                    result = cursor.fetchone()
                    print(f"查看的查询的数据", result)
                    return result
        except Exception as e:
            print(f"出错了", e)