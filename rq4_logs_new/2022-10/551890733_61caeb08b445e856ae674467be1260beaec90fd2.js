const morgan = require('morgan');
const express = require('express');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const mongoSanitize = require('express-mongo-sanitize');
const xss = require('xss-clean');
const hpp = require('hpp');

var app = express();

const tourRouter = require(`./routes/tour`);
const userRouter = require(`./routes/user`);

//GLOBAL MIDDLWARES

// Set security HTTP headers
app.use(helmet());

app.use(morgan('dev')); //3'rd party middleware

// Limit requests from same API
const limiter = rateLimit({
  max: 100,
  windowMs: 60 * 60 * 1000, //max 100 requests per hour
  message: 'Too many requests from this IP, please try again later.',
});

app.use('/api', limiter); //limit routes which starts with /api

// Body parser, reading data from body into req.body
app.use(express.json({ limit: '10kb' })); //middleware for body parser,

//Data Sanitization against NoSQL query injection
app.use(mongoSanitize());

//Data Sanitization against XSS
app.use(xss());

//Prevent paramater pollution
app.use(
  hpp({
    whitelist: [
      'duration',
      'ratingsQuantity',
      'ratingsAverage',
      'maxGroupSize',
      'difficulty',
      'price',
    ],
  })
);

//serving static files
app.use(express.static(`${__dirname}/public`));

//test middlewares
app.use((req, res, next) => {
  req.requestTime = new Date().toISOString();
  //console.log(req.headers);
  next();
});

app.use('/api/v1/users', userRouter);
app.use('/api/v1/tours', tourRouter);

module.exports = app;