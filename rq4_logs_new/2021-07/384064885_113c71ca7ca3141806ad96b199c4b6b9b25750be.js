var express = require('express');
var app  = express();

var mysql = require('mysql');
var bodyParser = require('body-parser');

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

var con = mysql.createConnection({
 
    host:'bghka8odsgyh8rbbk8nx-mysql.services.clever-cloud.com',
    user:'umv2wcvxi3nz5lnr',
    password:'gNE4J6IWZZdxDEkdYzsy',
    database: 'bghka8odsgyh8rbbk8nx'

});
  
var server = app.listen(1348, function(){
  var host = server.address().address
  var port = server.address().port
  console.log("server start",host,port);

});

con.connect(function(error){
  if(!!error) console.log(error,error.message);
  else console.log("connected");
});

app.get('/users', function(req, res){
  con.query('SELECT * FROM users', function(error, rows, fields){
        if(!!error) console.log(error,error.message);
        else{
            console.log(rows);
            res.send(rows);
        }
  });
});
/**
 * Donde tienes las tablas, ya es cuestion de
 */
app.post('/users', function (req, res){
  console.log("body:",req.body)
 // res.json(req.body);
  con.query('INSERT INTO users SET ?', req.body, function(error, rows, fields){
    if(!!error)console.log(error.message);
    else{
      console.log(rows);
      res.send(JSON.stringify(rows));
    }
  });
});

app.get('/users/:id', function(req, res){
  console.log(req.params.id);
  con.query('SELECT * FROM users WHERE id=?', req.params.id, function(error, rows, fields){
    if(!!error)console.log(error,error.message);
    else{
      console.log(rows);
      res.send(JSON.stringify(rows));
    }
  });
});

app.get('/users/:id', function(req, res){
  console.log(req.params.id);
  con.query('DELETE * FROM users WHERE id=?', req.params.id, function(error, rows, fields){
    if(!!error)console.log(error.message);
    else{
      console.log(rows);
      res.end('success delete!');
    }
  });
});

app.put('/users/:id', function (req, res){
  console.log("body:",req.body, req.params.id);
  //pasar varios parametros al update, sepasan como array, te recomiendo que utilices un ORM
  /**
   * Sequelize -> yarn add sequealize
   */
  con.query('UPDATE users SET ? WHERE id=?',[req.body,req.params.id, ], function(error, rows, fields){
    if(error) throw error;
      console.log(rows);
      res.end(JSON.stringify(rows));
  });
});