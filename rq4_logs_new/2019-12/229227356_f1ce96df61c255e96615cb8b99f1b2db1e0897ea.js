var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');
var cors = require('cors')
var authMiddleware = require('./src/middleware/auth')
var loginMiddleware = require('./src/middleware/login')
var findMyListMiddleware = require('./src/middleware/findMyList')
var findStockMiddleware = require('./src/middleware/findStock')
var findMyPartMiddleware = require('./src/middleware/findMyPart')
var findMySetsMiddleware = require('./src/middleware/findMySets')
var addPartMiddleware = require('./src/middleware/addPart')
var addSetMiddleware = require('./src/middleware/addSet')
var findPartAllMiddleware = require('./src/middleware/findPartAll')
var findPartCanUseAllMiddleware = require('./src/middleware/findPartCanUse')
var addListMiddleware = require('./src/middleware/addList')
var editProfileMiddleware = require('./src/middleware/editProfile')
var findUsersMiddleware = require('./src/middleware/findUsers')
var addUsersMiddleware = require('./src/middleware/addUsers')
var editUsersMiddleware = require('./src/middleware/editUsers')
var addMinmaxMiddleware = require('./src/middleware/addMinmax')
var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);
app.use(cors())

app.use('/auth', authMiddleware, function (req, res) {
  var result = 'Username or Password Invalid'
  res.json({ result: result }).end()
})

app.use('/addminmax', addMinmaxMiddleware, function (req, res) {
  
})
app.use('/addusers', addUsersMiddleware, function (req, res) {
  var result = 'Username In Use'
  res.json({ result: result }).end()
})

app.use('/editusers', editUsersMiddleware, function (req, res) {
  
})

app.use('/login', loginMiddleware, function (req, res) {

})

app.use('/editprofile', editProfileMiddleware, function (req, res) {

})

app.use('/addlist', addListMiddleware, function (req, res) {

})

app.use('/mylist', findMyListMiddleware, function (req, res) {
  /////next()
})

app.use('/findmypart', findMyPartMiddleware, function (req, res) {
  /////next()
})

app.use('/findusers', findUsersMiddleware, function (req, res) {
  /////next()
})
app.use('/findpart', findPartCanUseAllMiddleware, function (req, res) {
  /////next()
})

app.use('/findstock', findStockMiddleware, function (req, res) {

})

app.use('/findpartall', findPartAllMiddleware, function (req, res) {

})

app.use('/findmyset', findMySetsMiddleware, function (req, res) {

})
app.use('/addset', addSetMiddleware, function (req, res) {
  ////edit
  var result = 'Username or Password Invalid'
  res.json({ result: result }).end()
})

app.use('/addpart', addPartMiddleware, function (req, res) {
  var insertedCount = 0
  res.status(200).json({ insertedCount: insertedCount }).end()
})

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;