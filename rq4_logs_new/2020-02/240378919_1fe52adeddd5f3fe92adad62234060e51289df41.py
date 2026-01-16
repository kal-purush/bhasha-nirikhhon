import core

from SQL_variables import TABLES, TERMS, ATTRIBUTES, INSERTS

#Creating tables
create_tables = core.Create_tables()
create_tables.connect('host', 'username', 'password')
create_tables.use_db('name_db')
create_tables.creating_tables(TABLES)

#Get datas from openfoodfacts API
get_datas = core.Get_datas()

for term in TERMS:
    get_datas.get_json(term)

for attribute in ATTRIBUTES:
    get_datas.get_attribute(attribute)

datas_dictionnary = get_datas.set_dictionnary()

#Put datas in the tables
insert_datas = core.Insert_datas(datas_dictionnary, create_tables.cursor)

for insert in INSERTS:
    insert_datas.insert_datas_products(insert)

#Build interface for user





#Get favorites from user





