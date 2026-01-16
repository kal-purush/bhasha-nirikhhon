/// <reference path="typings/main.d.ts" />
/// <reference path="routes/main.ts" />
/// <reference path="models/main.d.ts" />
/// <reference path="common/util.ts" />

import log_ = require("./common/logger/index");
var logger = log_.getRunServiceLogger();

import Express = require('express');
import Path = require('path');
import ExpressSession = require("express-session");
import Passport = require("passport");

//import favicon = require('serve-favicon');
//import logger = require('morgan');
import CookieParser = require('cookie-parser');
import bodyParser_ = require('body-parser');

import RouteCollection = require('./routes/main');

import util_ = require("./common/util");

var app = Express();

app.set('views', Path.join(__dirname, 'views'));
app.set('view engine', 'ejs');
logger.info("setup view engine [%s], views [%s]", app.get("view engine"), app.get("views"));

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
//app.use(logger('dev'));
logger.info("use body parser json()");
app.use(bodyParser_.json());

logger.info("use body parser urlencoded()");
app.use(bodyParser_.urlencoded({ extended: false }));

logger.info("use cookie parser urlencoded()");
app.use(CookieParser());

logger.info("use static folder [%s]", Path.join(__dirname, 'public'));
app.use(Express.static(Path.join(__dirname, 'public')));

app.use(ExpressSession({
    secret: 'satori portal',
    resave: true,
    saveUninitialized: true, cookie: { secure: false, path: '/', httpOnly: false }
}));
app.use(Passport.initialize());
app.use(Passport.session());

RouteCollection.registerAll(app);

// catch 404 and forward to error handler
app.use(<Express.RequestHandler>function (req, res, next) {
    var err = <SatoriPortal.StatusError>new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
app.use(<Express.ErrorRequestHandler>function (err, req, res, next) {
    res.status(err.status || 500);
    // res.render('error', {
    //     message: err.message,
    //     error: util_.isProduction() ? {} : err
    // });
    res.render('error', {
        message: err.message,
        error: err
    });
});


module.exports = app;