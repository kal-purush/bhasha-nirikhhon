const createTable = `
    create extension if not exists "uuid-ossp";

    drop table if exists products;

    create table products (
        id uuid primary key default uuid_generate_v4(),
        title text not null,
        description text,
        price integer,
        src text
    );
`;

const insertInitData = `
    insert into products (title, description, price, src) values
        ('Christmas Tree', 'I wish you a Merry Christmas and Happy New Year!', 20, 'https://inspiredbycharm-wpengine.netdna-ssl.com/wp-content/uploads/2018/11/A-Nostalgia-Inspired-Christmas-Tree.jpg'),
        ('Spirit', 'I wish you a Marry Christmas!', 30, 'https://cdn.pixabay.com/photo/2017/12/01/16/14/cinnamon-stars-2991174__480.jpg'),
        ('Toy', 'I wish you a Marry Christmas!', 5, 'https://cdn.pixabay.com/photo/2016/11/12/22/42/santa-claus-1819933__480.jpg'),
        ('Balls', 'I wish you a Marry Christmas!', 10, 'https://images.pexels.com/photos/3224164/pexels-photo-3224164.jpeg?auto=compress&cs=tinysrgb&dpr=2&w=500'),
        ('Nutcracker', 'I wish you a Marry Christmas!', 99, 'https://images-na.ssl-images-amazon.com/images/I/81d4aAabmmL._AC_SL1500_.jpg'),
        ('Socks', 'I wish you a Marry Christmas!', 17, 'https://cdn.pixabay.com/photo/2015/09/09/18/25/feet-932346__480.jpg'),
        ('Mug', 'I wish you a Marry Christmas!', 29, 'https://images.pexels.com/photos/1693652/pexels-photo-1693652.jpeg?auto=compress&cs=tinysrgb&dpr=2&w=500'),
        ('Сinnamon', 'I wish you a Marry Christmas!', 80, 'https://images.pexels.com/photos/40887/anise-aroma-aromatic-brown-40887.jpeg?auto=compress&cs=tinysrgb&dpr=2&w=500');
`;

const getAll = "select * from products";

export const productsQuery = {
  createTable,
  insertInitData,
  getAll,
};