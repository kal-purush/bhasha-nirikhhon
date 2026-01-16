from backend.api import Api

class LandmasterApi(Api):
    def __init__(self):
        super().__init__()

    def getOffices(self, id_landlord):
        cursor = self._db.getCursor()

        sql = f"SELECT * FROM offices WHERE landlordId = '{id_landlord}'"
        cursor.execute(sql)

        return {
            'success': True,
            'offices': cursor.fetchall()
        }

    def excludeOffice(self, id_office):
        cursor = self._db.getCursor()

        sql = f"DELETE FROM offices WHERE officeId  = '{id_office}'"
        cursor.execute(sql)

        sql = f"SELECT * FROM offices WHERE officeId = '{id_office}'"
        cursor.execute(sql)

        db_office = cursor.fetchone()

        if db_office is not None:
            return {
                'success': False,
                'error': 'Office not excluded'
            }
        return{
            'sucess': True
        }

    def modifyOffice(self, office_info):
        cursor = self._db.getCursor()

        pre_update = f"SELECT * FROM offices WHERE officeId = {office_info['officeId']}"
        cursor.execute(pre_update)
        db_office_pre = cursor.fetchone()

        update = (
            f"UPDATE offices "
            f"SET landlordId = '{office_info['landlordId']}', "
            f"address = '{office_info['address']}', "
            f"district = '{office_info['district']}', "
            f"number = '{office_info['number']}', "
            f"description = '{office_info['description']}', "
            f"scoring = '{office_info['scoring']}', "
            f"nScore = '{office_info['nScore']}', "
            f"daily_rate = '{office_info['daily_rate']}', "
            f"type = '{office_info['type']}' "
            f"WHERE officeId = {office_info['officeId']}")

        cursor.execute(update)

        post_update = f"SELECT * FROM offices WHERE officeId = {office_info['officeId']}"
        cursor.execute(post_update)
        db_office_post = cursor.fetchone()

        if db_office_post is None:
            return {
                'success': False,
                'error': 'Office not found'
            }
        if db_office_pre == db_office_post:
            return {
                'success': False,
                'error': 'Office not modified'
            }
        return{
            'success': True,
            'office': db_office_post
        }