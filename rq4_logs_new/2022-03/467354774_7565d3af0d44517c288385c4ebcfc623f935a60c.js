var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');
let Parser = require("rss-parser");
let parser = new Parser();
require('dotenv').config();
var mongoose = require('mongoose');
var Entry = require('./models/schemas');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'hbs');

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

// create connection to database
mongoose.connect(process.env.DB_URI, {useNewUrlParser:true, useUnifiedTopology:true})
.then ( () => {
  (async () => {
    let feed = await parser.parseURL('https://rss.app/feeds/APFa1G3Jdm04A819.xml');
    feed.items.forEach(item => {
      // console.log(item)
      const instance = new Entry();
      instance.title = item.title;
      instance.content = item.content;
      instance.link = item.link;
      instance.pubDate = item.pubDate;
      instance.guid = item.guid;
      instance.save();
    });
  })(
  );
  console.log('Db connected');
})
.catch( (err) => {
  console.log(err);
});




module.exports = app;