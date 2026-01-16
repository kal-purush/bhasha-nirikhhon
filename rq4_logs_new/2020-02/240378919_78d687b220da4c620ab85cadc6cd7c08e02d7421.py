import mysql.connector
from mysql.connector import errorcode


class InitSQL:

    def __init__(self, host, user, password, name_database):
        self.cnx = mysql.connector.connect(host=host, 
                                    user=user, 
                                    password=password, 
                                    database=name_database)
        self.cursor = self.cnx.cursor()


class TablePurchaseStores:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Purchase_stores (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        store_name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
        ) ENGINE=InnoDB"""
        self.sql_query_insert_into = """INSERT INTO Purchase_stores
        (store_name)
        VALUES (%(store_name)s)"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
    
    def insert_into_table(self, cnx, cursor, datas):
        stores = []
        for data in datas:
            for attribute in data:
                purchase_store = attribute['stores']
                purchase_stores = purchase_store.split(',')
                for store in purchase_stores:
                    stores.append(store)

        stores = list(dict.fromkeys(stores))
        for store in stores:
            store_to_add = {'store_name': store}
            cursor.execute(self.sql_query_insert_into, store_to_add)

        cnx.commit()


class TableCategories:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Categories(
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        category_name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
        ) ENGINE=InnoDB"""
        self.sql_query_insert_into = """INSERT INTO Categories
        (category_name)
        VALUES(%(category_name)s)"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
    
    def insert_into_table(self, cnx, cursor, datas):
        categories_list = []
        for data in datas:
            for attribute in data:
                category = attribute['categories']
                categories = category.split(',')
                for category in categories:
                    categories_list.append(category)

        categories_list = list(dict.fromkeys(categories_list))
        for category in categories_list:
            category_to_add = {'category_name': category}
            cursor.execute(self.sql_query_insert_into, category_to_add)

        cnx.commit()


class TableProducts:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Products (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        product_name VARCHAR(255) NOT NULL,
        nutriscore VARCHAR(1) NOT NULL,
        link TEXT NOT NULL,
        details TEXT,
        PRIMARY KEY (id)
        ) ENGINE=InnoDB"""
        self.sql_query_insert_into = """INSERT INTO Products
        (product_name, nutriscore, link, details)
        VALUES (%(product_name)s, %(nutriscore)s, %(link)s, %(details)s)"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)

    def insert_into_table(self, cnx, cursor, datas):
        for data in datas:
            for attribute in data:
                attributes = {
                    'product_name': attribute['product_name_fr'],
                    'nutriscore': attribute['nutrition_grades_tags'][0],
                    'link': 'https://world.openfoodfacts.org/product/{}'.format(attribute['code']),
                    'details': attribute['generic_name_fr']
                }
                cursor.execute(self.sql_query_insert_into, attributes)

        cnx.commit()

class TableFavorites:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Favorites (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        product_id_replaced INT UNSIGNED NOT NULL,
        product_id_replacement INT UNSIGNED NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT fk_product_id_replaced
            FOREIGN KEY (product_id_replaced)
            REFERENCES Products(id),
        CONSTRAINT fk_product_id_replacement
            FOREIGN KEY (product_id_replacement)
            REFERENCES Products(id)
        ) ENGINE=InnoDB"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)


class TableProductCategories:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Product_categories (
        product_id INT UNSIGNED NOT NULL,
        category_product_id INT UNSIGNED NOT NULL,
        CONSTRAINT fk_product_id_cat
            FOREIGN KEY (product_id)
            REFERENCES Products(id),
        CONSTRAINT fk_category_product_id
            FOREIGN KEY (category_product_id)
            REFERENCES Categories(id)
        ) ENGINE=InnoDB"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)


class TableProductStores:

    def __init__(self):
        self.sql_query_create_table = """CREATE TABLE Product_stores (
        product_id INT UNSIGNED NOT NULL,
        purchase_store_id INT UNSIGNED NOT NULL,
        CONSTRAINT fk_product_id_stores
            FOREIGN KEY (purchase_store_id)
            REFERENCES Purchase_stores(id)
        ) ENGINE=InnoDB"""

    def create_table(self, cursor):
        try:
            cursor.execute(self.sql_query_create_table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)


class Execute_SQL_methods:

    def __init__(self, cursor, tables_object):
        self.tables_object = tables_object
        self.cursor = cursor

    def creating_tables(self):
        for table in self.tables_object:
            table.create_table(self.cursor)