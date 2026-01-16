/// <reference path="types/express.d.ts" />
/// <reference path="types/mongoose.d.ts" />
/// <reference path="types/node.d.ts" />

import  * as express from "express";
import *  as path from "path";
import *  as favicon from "serve-favicon";
import *  as logger from "morgan";
import *  as cookieParser from "cookie-parser";
import *  as bodyParser from "body-parser";
import mongoose = require('mongoose');
import methodOverride = require("method-override");

import * as project from "./routes/project.route";
var app = express();
console.log('ping');

app.use(favicon(__dirname + '/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(methodOverride('_method'));

var lpath = __dirname.substring(0, __dirname.indexOf("server"));
console.log(lpath);

app.use(express.static(path.join(lpath, 'src')));
app.use(express.static(path.join(lpath, 'models')));
app.use(express.static(path.join(lpath, 'node_modules/angular2/bundles')));
app.use(express.static(path.join(lpath, 'node_modules')));
app.use(express.static(path.join(lpath, 'node_modules/systemjs/dist')));
app.use(express.static(path.join(lpath, 'node_modules/es6-shim')));
console.log(path.join(lpath, 'src'));



// ALL ROUTES HERE!!!!


//app.get('/projects', project.list);
app.post( '/api/projects', project.save);
app.get(  '/api/projects', project.load);
app.get( '/api/projects/:projectId', project.loadById);


              
// NO ROUTES BELOW HERE !!!!

// catch 404 and forward to error handler
app.use(function(req:any , res : any , next :any) {
  console.log("inside 404");
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err:any , req:any, res:any, next:any) {
    res.status(err.status || 500);
    console.log("inside 500");
    console.log(err.message);
    res.render('error', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err: any, req: any, res: any, next: any) {
  res.status(err.status || 500);
  console.log("inside 500b");
  console.log(err.message);
  res.render('error', {
    message: err.message,
    error: {}
  });
});


//var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/rxjsdemo', function(err: any) {
  if (err) {
    console.log('connection error', err);
  } else {
    console.log('connection successful');
  }
});



module.exports = app;
//startup.startup();
console.log("pong");