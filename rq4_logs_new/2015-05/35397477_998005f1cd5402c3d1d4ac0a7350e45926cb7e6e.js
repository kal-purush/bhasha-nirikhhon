var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var handlebars = require('express-handlebars');
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);

//Configures the Template engine
app.engine('handlebars', handlebars({defaultLayout: 'layout'}));
app.set('view engine', 'handlebars');
app.set('views', __dirname + '/views');
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

//set environment ports and start application
app.set('port', process.env.PORT || 3000);

// Routes
app.get('/', function(req, res){
  res.render('index');
});

http.listen(app.get('port'), function() {
    console.log('Express server listening on port ' + app.get('port'));
});