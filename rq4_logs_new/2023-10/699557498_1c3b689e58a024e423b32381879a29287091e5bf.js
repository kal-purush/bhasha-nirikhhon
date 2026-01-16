const express = require('express');
const app = require('express')();

require('dotenv').config();
const path = require('path');
const cors = require('cors');
const usersRoutes = require('./routes/usersRoutes');

app.use(express.json());
app.use(express.urlencoded({extended: true}));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static('public'));
app.use(cors());

//Leemos las rutas como un middelware
app.use('/users', usersRoutes);

//Solicitud de recursos mediante GET
app.get('/', (req, res) => {
    res.render('index.html');
});

app.get('/public/style.css', function(req, res){
  res.sendFile(__dirname + '/public/style.css');
});

app.get('/', (req, res) => {
    res.render('form');
    res.sendFile(__dirname + '/registro.html');
    res.sendFile("index.html");
  });
  
//app.post('/', (req,res) => {
//    var username = req.body.username;
//    var htmlData = 'Hello:' + username;
//    res.send(htmlData);
//    console.log(htmlData);
//});

module.exports = app;