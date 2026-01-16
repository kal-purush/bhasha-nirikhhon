const { Block } = require('./class/Block');

var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');


var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
    // set locals, only providing error in development
    res.locals.message = err.message;
    res.locals.error = req.app.get('env') === 'development' ? err : {};

    // render the error page
    res.status(err.status || 500);
    res.render('error');
});

module.exports = app;


let transactions1 = ["i am a bad man 1", "I love blockchain 1"];
let block1 = new Block(0, transactions1);
console.log("Bloack1", block1.blockHash);

let transactions2 = ["i am a bad man 2", "I love blockchain 2"];
let block2 = new Block(block1.blockHash, transactions2);
console.log("Bloack2", block2.blockHash);

let transactions3 = ["i am a bad man 3", "I love blockchain 3"];
let block3 = new Block(block2.blockHash, transactions3);
console.log("Bloack3", block3.blockHash);

let transactions4 = ["i am a bad man 4", "I love blockchain 4"];
let block4 = new Block(block3.blockHash, transactions4);
console.log("Bloack4", block4.blockHash);