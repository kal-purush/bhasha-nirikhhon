var express = require('express');
var app = express();
var mysql = require('mysql');
var passport = require('passport');
var flash = require('connect-flash');
var session = require('express-session');
var Sequelize = require('sequelize');
var bodyParser = require('body-parser');

var sequelize = new Sequelize('mydb', 'root', 'P@ssw0rd', {
    host: 'localhost',
    dialect: 'mysql'
})


app.use(bodyParser.json()); // get information from html forms
app.use(bodyParser.urlencoded({ extended: true }));

// required for passport
app.use(session({
    secret: 'ilovescotchscotchyscotchscotch',
    resave: false,
    saveUninitialized: true
})); // session secret
app.use(passport.initialize());
app.use(passport.session()); // persistent login sessions
app.use(flash()); // use connect-flash for flash messages stored in session

require('./config/passport')(app,passport);

app.set('view engine', 'ejs');

// routes
require('./app/routes.js')(app, passport);

var server = app.listen(8081, "localhost", function () {
    var host = server.address().address
    var port = server.address().port
    console.log("Example app listening at http://%s:%s", host, port)
});