from app.handler.fatcory import PityResponse
from app.utils.time_control import now_time
from app.utils.yxtxClient import DBClient


class divideClassFlagService():

    @classmethod
    def checkxbClassFlag(cls, courseVersion):
        print(courseVersion)
        courseDB = DBClient().getShopDBClient()
        cursorDB = courseDB.cursor()
        nowTime = now_time()
        sql = """ SELECT  t. * FROM yxtx_course_test.class_info t WHERE course_version_id = %s and  pause_flag =0 and full_flag =0 and class_start_time > %s"""
        # 使用 execute()  执行SQL
        cursorDB.execute(sql, [courseVersion, nowTime])
        courseDB.commit()
        # 获取所有记录列表
        results = cursorDB.fetchall()
        if len(results):
            return PityResponse.success_check_xb()
        else:
            return PityResponse.success_check_xbFail()

    @classmethod
    def checkjjClassFlag(cls, goodsId):
        flag = cls.checkGoodBatch(goodsId=goodsId)
        if flag['msg'] == "校验成功,存在可接量班级":
            return PityResponse.success_check_jj()
        else:
            result = cls.getMainGoodsBatch(goodsId=goodsId)
            return result

    @classmethod
    def checkGoodBatch(cls, goodsId):
        courseDB = DBClient().getCourseDBClient()

        cursorCourseDB = courseDB.cursor()
        nowTime = now_time()
        sql = """ SELECT  t. * FROM yxtx_course_test.advanced_receive_batch t WHERE 
        goods_id = %s  and pause_flag = 0 and full_flag =0 and %s BETWEEN receive_start_time  and receive_end_time"""
        cursorCourseDB.execute(sql, [goodsId, nowTime])

        courseDB.commit()
        # 获取所有记录列表
        courseResults = cursorCourseDB.fetchall()
        if courseResults:
            return PityResponse.success_check_jj()
        else:
            return PityResponse.success_check_jjFail()

    @classmethod
    def getMainGoodsBatch(cls, goodsId):
        shopDB = DBClient().getShopDBClient()
        cursorDB = shopDB.cursor()
        sql = """ SELECT  t. * FROM yxtx_shop_test.shop_goods t WHERE id = %s and goods_type =%s and off_shelf_flag = %s"""
        # 使用 execute()  执行SQL
        cursorDB.execute(sql, [goodsId, 3, 0])

        shopDB.commit()
        # 获取所有记录列表
        results = cursorDB.fetchall()
        if not results:
            return PityResponse.success_check_goodsIdFail()
        mainGoods = results[0][31]
        if mainGoods == 0:
            return PityResponse.success_check_jjFail()

        return cls.checkGoodBatch(goodsId=mainGoods)

    @classmethod
    def checkGoodsIDCourseClass(cls, goodsId):
        courseVersionId = cls.getGoodsIDCourseVersionID(goodsId)
        if courseVersionId is None:
            return PityResponse.success_check_goodsIdFail()
        return cls.checkxbClassFlag(courseVersionId)


    @classmethod
    def getGoodsIDCourseVersionID(cls, goodsId):
        shopDB = DBClient().getShopDBClient()
        cursorDB = shopDB.cursor()
        sql = """SELECT sgc.*, sg.*
                 FROM yxtx_shop_test.shop_goods_course sgc inner join
                    yxtx_shop_test.shop_goods sg
                 on sgc.goods_id = sg.id
                    and sgc.goods_type = %s
                    and sgc.del_flag = %s
                    and sgc.goods_id = %s
                    and sg.off_shelf_flag = %s"""

        cursorDB.execute(sql, [1, 0, goodsId, 0])
        shopDB.commit()
        results = cursorDB.fetchall()
        if results is True:
            return results[0][5]
        return None

if __name__ == '__main__':
    a = divideClassFlagService.getGoodsIDCourseVersionID(764)
