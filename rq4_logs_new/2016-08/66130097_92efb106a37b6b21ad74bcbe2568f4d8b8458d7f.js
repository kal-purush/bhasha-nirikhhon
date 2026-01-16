
/**
 * Module dependencies.
 */

var express = require('express');
var routes = require('./routes');
var http = require('http');
var path = require('path');

//load lecturers
var lecturers = require('./routes/lecturers');
var app = express();
var connection = require('express-myconnection');
var mysql = require('mysql');

// all environments
app.set('port', process.env.PORT || 3000);
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.json());
app.use(express.urlencoded());
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

app.use(
    connection(mysql, {
      host      : 'localhost',
      user      : 'root',
      password  : 'root',
      port      : 3306,
      database  : 'campus'
    }, 'request')
);

//routing
app.get('/', routes.index);
app.get('/lecturers', lecturers.list);
app.get('/lecturers/add', lecturers.add);
app.post('/lecturers/add', lecturers.save);
app.get('/lecturers/edit/:id', lecturers.edit);
app.post('/lecturers/edit/:id', lecturers.save_edit);
app.get('/lecturers/delete/:id', lecturers.delete);

app.use(app.router);

http.createServer(app).listen(app.get('port'), function(){
  console.log('Express server listening on port ' + app.get('port'));
});