import pymysql
import config


class MysqlHepler:
    def __init__(self):
        self.db = pymysql.connect(config.devUrl, config.devUserName, config.devPassward, config.database_name,
                                  charset="utf8")
        self.cursor = self.db.cursor()

    # 查询表整个表
    def get_table_data(self, table_name):
        sql = "select * from " + table_name
        table_data_list = self.cursor.fetchall()
        return table_data_list

    # 根据sql语句查询一个
    def get_one_by_sql(self, sql):
        self.cursor.execute(sql)
        one = self.cursor.fetchone()
        return one

    # 根据sql查询所有
    def get_all_by_sql(self, sql):
        self.cursor.execute(sql)
        all = self.cursor.fetchall()
        return all

    #
    # 更新 例如: update_sql = "update bs_car_series set en_name = '%s' where id = %d" % (en_name, id)
    def update_db(self, sql):
        print(sql)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            print("成功")
        except:
            print("失败")
            self.db.rollback()