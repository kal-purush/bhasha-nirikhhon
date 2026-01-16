'use strict';

import express = require('express');
import bodyParser = require('body-parser');
import cookieParser = require('cookie-parser');
import path = require('path');

import RequestError = require('./errors/request');
import routes = require('./routes/index');
import callback = require('./routes/callback');

let app = express();
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(bodyParser.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', routes);
app.use('/redirect', callback);

// Catch 404 and forwarding to error handler.
app.use(function (req, res, next) {
  next(new RequestError('Not Found', 404));
});

app.use(<express.ErrorRequestHandler>function (error, req, res, next) {
  res.status(error.status || 400);
  res.render('error', {
    message: error.message,
    error: {
      status: error.status,
      details: error.stack
    }
  });
});

export = app;