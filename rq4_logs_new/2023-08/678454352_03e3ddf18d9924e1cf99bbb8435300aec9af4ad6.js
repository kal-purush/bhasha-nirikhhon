//  npm install express --save
//  npm install mysql
//  npm i body-parser
// npm i cors

import express from 'express';
import pkg from 'body-parser';
const { urlencoded, json } = pkg;


var app = express();
app.use(urlencoded({ extended: false }));
app.use(json())
import cors from 'cors';

var port = 8081;

// Create Database Connection
import { createConnection } from 'mysql';
const mysqlConnection = createConnection({
   host: "localhost",
   user: "root",
   password: "",
   database: "test",
});
mysqlConnection.connect((err) => {
   if (err) {
      console.log(err);
      throw err;
      
   }
   console.log("MySql Connected");
});


app.use(function (req, res, next) {

   // Website you wish to allow to connect
   res.setHeader('Access-Control-Allow-Origin', 'http://localhost:3000');

   // Request methods you wish to allow
   res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');

   // Request headers you wish to allow
   res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');

   // Set to true if you need the website to include cookies in the requests sent
   // to the API (e.g. in case you use sessions)
   res.setHeader('Access-Control-Allow-Credentials', true);
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

   // Pass to next layer of middleware
   next();
});

app.use(
   cors({ origin: ['http://localhost:8081', 'http://127.0.0.1:8081'] })
);

app.get('/get_users', async function (req, res) {
   mysqlConnection.query('SELECT * from users', (err, rows, fields) => {
      if (err) {
         throw err;
      }
      res.send(rows);
   });
}
);


app.post('/login', async function (req, res) {

   const query = 'SELECT * ' +
      'FROM `users` ' +
      'WHERE `email` = ? and password= ?';
   const value = [req.body.email, req.body.password];


   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log("rows")
      console.log(rows.length)

      if (rows.length > 0) {
         res.send(
            {
               "success": true,
               "user_details": rows
            }
         );
      } else {
         res.send({ "success": false });
      }

   });
}
);


app.post('/insert_product', async function (req, res) {

   
   let query = `INSERT INTO product 
   (product_name, product_price,quantity,discount,product_image,product_description,final_price) VALUES (?, ?,?,?,?,?,?);`;

   const value = [req.body.product_name, req.body.product_price, req.body.quantity, req.body.discount, req.body.product_image, req.body.product_description,req.body.final_price];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log(rows);
      console.log("Row inserted with id = " + rows.insertId);

      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // res.send(rows);
   });
}
);


app.get('/get_product_list', async function (req, res) {

   let query = `select * from product;`;


   mysqlConnection.query(query, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log("Row inserted with id = "
         + rows.insertId);
      res.send(rows);
   });
}
);

app.post('/add_cart', async function (req, res) {

   let query = `INSERT INTO cart
   (product_id,user_id) VALUES (?,?)`;
   // SELECT products.product_id,users.user_id,orders.* from cart 
   // inner join users 
   //    on 
   //    cart.user_id= orders.user_id
   // where
   // orders.date >= ?
   // and
   // orders.date <= ?`;

   const value = [req.body.product_id,req.body.user_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log("Row inserted with id = "
         + rows.insertId);
         if (rows.affectedRows > 0) {
            res.send({ "success": true });
         } else {
            res.send({ "success": false });
         }
      
   });
}
);
app.post('/delete_cart', async function (req, res) {

   let query = `delete FROM cart WHERE product_id=? AND user_id=?`;

   const value = [req.body.product_id,req.body.user_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // console.log("Row inserted with id = "
         // + rows.insertId);
      // res.send(rows);
   });
}
);

app.post('/delete_product', async function (req, res) {

   let query = `delete FROM product WHERE product_id=?`;

   const value = [req.body.product_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // console.log("Row inserted with id = "
         // + rows.insertId);
      // res.send(rows);
   });
}
);

app.post('/delete_cart_single_product', async function (req, res) {

   let query = `delete FROM cart WHERE product_id=? limit 1`;

   const value = [req.body.product_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // console.log("Row inserted with id = "
         // + rows.insertId);
      // res.send(rows);
   });
}
);


app.post('/search_product', async function (req, res) {

   let query = 'select * from product where product_name like "%'+req.body.search+'%"';
   console.log(query);

   mysqlConnection.query(query, (err, rows, fields) => {
      if (err) {
         throw err;      }
     
      res.send(rows);
   });
}
);


app.post('/show_cart', async function (req, res) {

   let query = 'SELECT COUNT( product.product_id) as count,product.* FROM product join cart on cart.product_id=product.product_id WHERE cart.user_id=? GROUP BY product.product_id';
   console.log(query);
   const value = [req.body.user_id];
   mysqlConnection.query(query, value, (err, rows, fields) => {

      if (err) {
         throw err;      }
     
      res.send(rows);
   });
}
);

app.post('/get_product', async function (req, res) {

   let query = 'select * from product where  product_id=?';
   console.log(query);

   const value = [req.body.product_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log("Row inserted with id = "
         + rows.insertId);
      res.send(rows);
   });
}
);
app.post('/signup', async function (req, res) {

   
   let query = `INSERT INTO users 
   (first_name,last_name,email,password,mobile_number,address,pincode) VALUES (?,?,?,?,?,?,?);`;

   const value = [req.body.first_name, req.body.last_name, req.body.email, req.body.password,req.body.mobile_number,req.body.address,req.body.pincode];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log(rows);
      console.log("Row inserted with id = " + rows.insertId);

      if (rows.affectedRows > 0) {
         res.send({ "success": true });
        
      } else {
         res.send({ "success": false });
         
      }
      // res.send(rows);
   });
}
);

app.post('/update_product', async function (req, res) {

   
   let query = `update product set product_name= ?, product_price =?,quantity=?,discount=?,product_image=?,product_description =?,final_price=? where product_id=?`;

   const value = [req.body.product_name, req.body.product_price, req.body.quantity, req.body.discount, req.body.product_image, req.body.product_description,req.body.final_price,req.body.product_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log(rows);
      console.log("Row inserted with id = " + rows.insertId);

      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // res.send(rows);
   });
}
);

app.post('/place_order', async function (req, res) {

   
   let query = `INSERT INTO orders (user_id,order_details,total_payable) VALUES (?, ?,?);`;

   const value = [req.body.user_id, req.body.order_details, req.body.total_payable];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log(rows);
      console.log("Row inserted with id = " + rows.insertId);

      if (rows.affectedRows > 0) {
         res.send({ "success": true });
      } else {
         res.send({ "success": false });
      }
      // res.send(rows);
   });
}
);

app.post('/report', async function (req, res) {

   let start_date=req.body.start_date+ " 00:00:00";
   let end_date=req.body.end_date+ " 23:59:59";
   let query = `SELECT users.first_name,users.last_name,users.mobile_number,orders.* from orders 
   inner join users 
      on 
      users.id= orders.user_id
   where
   orders.date >= ?
   and
   orders.date <= ?`;

   const value = [start_date,end_date];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      
      }
      console.log(rows);
         res.send(rows);
       
      // res.send(rows);
   });
}
);
app.post('/order_status', async function (req, res) {

   
   let query = `UPDATE orders set order_status=? WHERE order_id=?;`;

   const value = [req.body.order_status,req.body.order_id];

   mysqlConnection.query(query, value, (err, rows, fields) => {
      if (err) {
         throw err;
      }
      console.log(rows);
      
      if (rows.affectedRows > 0) {
         res.send({ "success": true });
         
      } else {
         res.send({ "success": false });
      }
      // res.send(rows);
   });
}
);


// start the server
app.listen(port);
console.log('Server started! At http://localhost:' + port);