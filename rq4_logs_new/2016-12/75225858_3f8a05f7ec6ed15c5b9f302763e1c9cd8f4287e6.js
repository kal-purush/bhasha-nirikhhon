var express = require('express');
var load = require('express-load');
var bodyParser = require('body-parser');

var app = express();

app.set('views'. __dirname + '/views');
app.set('view engine', 'ejs');
app.use(express.static(__dirname + '/public'));
app.get('/');
app.use(express.static(__dirname + '/public'));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

load('models')
    .then('controllers')
    .then('routes')
    .into(app);

app.listen(3000, function(){
   console.log("Servidor Node ok.");
});

module.exports = app;