PURCHASE_STORES = """CREATE TABLE Purchase_stores (
   id INT UNSIGNED NOT NULL AUTO_INCREMENT,
   Store_name VARCHAR(255) NOT NULL,
   PRIMARY KEY (id)
   ) ENGINE=InnoDB"""

CATEGORIES = """CREATE TABLE Categories(
   id INT UNSIGNED NOT NULL AUTO_INCREMENT,
   Category_name VARCHAR(255) NOT NULL,
   PRIMARY KEY (id)
   ) ENGINE=InnoDB"""

PRODUCTS = """CREATE TABLE Products (
   id INT UNSIGNED NOT NULL AUTO_INCREMENT,
   Product_name VARCHAR(255) NOT NULL,
   Nutriscore VARCHAR(1) NOT NULL,
   Links TEXT NOT NULL,
   Details TEXT,
   Category_id INT UNSIGNED NOT NULL,
   Store_id INT UNSIGNED NOT NULL,
   PRIMARY KEY (id),

   CONSTRAINT fk_categories_id 
        FOREIGN KEY (Category_id)
        REFERENCES Categories(id),

   CONSTRAINT fk_store_id
        FOREIGN KEY (Store_id)
        REFERENCES Purchase_stores(id)

) ENGINE=InnoDB"""

FAVORITES = """CREATE TABLE Favorites (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    Product_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_product_id 
        FOREIGN KEY (Product_id)
        REFERENCES Products(id)

) ENGINE=InnoDB"""

TABLES = [PURCHASE_STORES, CATEGORIES, PRODUCTS, FAVORITES]