import sqlite3

#creando base de datos
def create_or_get_database():
    conn = sqlite3.connect('restaurant.db')
    print("Database connected")
    return conn

#metodo para crear tablas
def create_table(conn):
    sql = '''
        CREATE TABLE IF NOT EXISTS User (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            email VARCHAR NOT NULL,
            phone INTEGER NOT NULL,
            passowrd VARCHAR NOT NULL,
            is_loged INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS Dish (
            dish_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR NOT NULL,
            description TEXT NOT NULL,
            price DOUBLE NOT NULL,
            ingredients VARCHAR NOT NULL,
            preparation_time INTEGER NOT NULL,
            ois_available INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS Order (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR NOT NULL,
            notes TEXT,
            preparation_time INTEGER NOT NULL,
            total DOUBLE NOT NULL,
            user_id INTEGER,
            CONSTRAINT User
            FOREIGN KEY (user_id)
            REFERENCES User(user_id),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS Order_Dish (
            order_dish_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            CONSTRAINT Order
            FOREIGN KEY (order_id)
            REFERENCES Order(order_id),
            dish_id INTEGER,
            CONSTRAINT Dish
            FOREIGN KEY (dish_id)
            REFERENCES Dish(dish_id),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    '''
    conn.execute(sql)
    print("All the tables have been created succesfully")


def main():
  conn = create_or_get_database()
  create_table(conn)