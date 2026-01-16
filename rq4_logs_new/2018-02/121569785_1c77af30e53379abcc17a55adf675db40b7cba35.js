var express = require("express");

var path = require("path");

var app = express();

var bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({extended: true}));

app.use(express.static(path.join(__dirname, "./static")));

app.set('views', path.join(__dirname, './views'));
app.set('view engine', 'ejs');

app.get('/', function (request, response) {
    response.render('index');
});
app.get('/kelvin', function (request, response) {
    response.render("kelvin");
});

app.get('/crystabel', function (request, response) {
    response.render("crystabel");
});

app.get('/brice', function (request, response) {
    response.render("brice");
});

app.get('/greetings', function (request, response) {
    response.render("greetings");
});

app.get('/greetings/KELVIN', function (request, response) {
    response.render("greetings/kelvin")
});

app.get('/greetings/crystabel', function (request, response) {
    response.render("greetings/crystabel")
});

app.get('/welcome', function (request, response) {
    response.render("welcome");
});

app.get('/welcome/brian', function (request, response) {
    response.render("welcome/brian");
});

app.get('/goodbye', function (request, response) {
    response.render("goodbye");
});

app.get('/goodbye/crystabel', function (request, response) {
    response.render("goodbye/crystabel");
});

app.get('/howdy', function (request, response) {
    response.render("howdy");
});

app.get('/howdy/kelvin', function (request, response) {
    response.render("howdy/kelvin");
});

app.get('/howdy/crystabel', function (request, response) {
    response.render("howdy/crystabel");
});

app.get('/hi', function (request, response) {
    response.render("hi");
});

app.get('/hi/CLINT', function (request, response) {
    response.render("hi/CLINT");
});

var port = 8888;

app.listen(port, function() {
    console.log("listening on port:", port);
});