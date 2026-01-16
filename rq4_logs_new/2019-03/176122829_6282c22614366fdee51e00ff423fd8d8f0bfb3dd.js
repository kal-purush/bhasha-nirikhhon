var express = require('express');

const bodyParser= require('body-parser');
var app = express();
var db = require('./db');
var user = require('./user');

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

app.use(function(req, res, next) { //allow cross origin requests
  res.setHeader("Access-Control-Allow-Methods", "*");
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Credentials", true);
  next();
});

app.post('/users', user.createUsers);
app.get('/users', user.seeResults);
app.delete('/users/:id', user.delete);

app.listen(3000, function () {
  console.log('listening on port 3000!');
});