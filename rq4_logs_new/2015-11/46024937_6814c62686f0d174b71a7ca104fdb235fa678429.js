var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

// mongogdb
var mongo = require('mongodb');


var routes = require('./routes/index');
var users = require('./routes/users');
var api = require('./routes/api');
var live = require('./routes/live');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
//app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(require('stylus').middleware(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

app.use('/api-doc', express.static(path.join(__dirname, 'public/swagger-editor'),'public'));

app.use('/', routes);
app.use('/users', users);
app.use('/api', api);
app.use('/live', live);
// catch 404 and forward to error handler
app.use(function (req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function (err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
//app.use(function (err, req, res, next) {
//    res.status(err.status || 500);
//    req.db = db;
//    console.log('db ' + db);
//    res.render('error', {
//        message: err.message,
//        error: {}
//    });
//});

// Make our db accessible to our router
//app.use(function (req, res, next) {
//    req.db = db;
//    next();
//});
var CronJob = require('cron').CronJob;
//Seconds: 0-59
//Minutes: 0-59
//Hours: 0-23
//Day of Month: 1-31
//Months: 0-11
//Day of Week: 0-6
new CronJob('00 56 16 * * *', function() {
    console.log('You will see this message every second');
}, null, true, null);

module.exports = app;

var port = 3000;
app.listen(port);
console.log('listen on port ' + port);