from sqlalchemy import create_engine
from sqlalchemy import text 
import psycopg2

class DataBase:
    query = {
        'create_company': text('insert into company(name, description)value(:name, :description)'),
        'max_company_id': text('select MAX(id) from company'),
        'delete_company': text('delete from company where company id = :company_id'),
        'list_SELECT': text('select * from employee where company_id = :id'),
        'item_SELECT':  text('select * from employee where company_id = :c_id and id = :e_id'),
        'maxID_SELECT':  text('select MAX(id) from employee where company_id = :c_id'), 
        'item_DELETE':  text('delete from employee where id = :id_delete'),
        'item_UPDATE':  text('update employee set first_name = :new_name where id = :employer_id'),
        'item_INSERT':  text('insert into employee(company_id, first_name, last_name, phone) values(:id, :name, :surname) where company_id = :id'),
    }

    def __init__(self, engine) -> None:
        self.db = create_engine(engine)

    def create_company(self, company_name:str, description:str):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['create_company'],
                                            parameters=dict(name=company_name, description=description))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD connection closed")

    def delete (self, company_id: int):
        try:
            with self.db.connect() as connection:
                connection.execute(self.query['delete_company'], parameters=dict(company_id=company_id))
                connection.commit()
                return result
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD connection closed")


    def last_company_id(self):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['max_company_id']).fetchall()[0][0]
            return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD connection closed")


    def get_last_employer(self, company_id: int):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['list_SELECT'],
                                            parameters=dict(id=company_id))
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD connection closed")

    def create_employer(self, company_id:int, first_name:str, last_name:str, phone:str):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['item_INSERT'],
                                            parameters=dict (id=company_id, name=first_name, surname=last_name,
                                                            phone_num=phone))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD connection closed")

    def get_list_employer(self, company_id:str):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['list_SELECT'],
                                            parameters=dict(id-company_id))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD cjnnection closed")
            
    def create_employer(self, company_id:int, first_name: str, last_name:str, phone:str):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['item_INSERT'],
                                            parameters=dict(id=company_id, name=first_name, surname=last_name,
                                                            phone_num=phone))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD cjnnection closed")
        
    def get_employer_id(self, company_id:int):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['maxID_SELECT'],
                                            parameters=dict(c_id=company_id))
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD cjnnection closed")

    def update_emploter_info(self, new_name:str, id:int):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['item_UPDATE'],
                                            parameters=dict(new_name=new_name, eployer_id=id))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD cjnnection closed")

    def delete_employer(self, id:int):
        try:
            with self.db.connect() as connection:
                result = connection.execute(self.query['item_DELETE'],
                                            parameters=dict(id_delete=id))
                connection.commit()
                return result 
        except Exception as _ex:
            print ("[INFO] Error SQL", _ex)
        finally:
            if connection:
                connection.close()
                print ("[INFO] BD cjnnection closed")


        