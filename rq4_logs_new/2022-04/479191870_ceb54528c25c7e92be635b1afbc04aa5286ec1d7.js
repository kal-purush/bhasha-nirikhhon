const express = require('express');
const req = require('express/lib/request');
const res = require('express/lib/response');
const app = express();

app.set('view engine', 'ejs');

app.get('/', (req,res) => {
    res.render("home.ejs")
    return res.send({ message: 'Tudo OK com este método get'}); 
});

app.post('/', (req,res) => {
    return res.send({ message: 'Tudo OK com este método post'});
});

app.listen(3000);

module.exports = app;