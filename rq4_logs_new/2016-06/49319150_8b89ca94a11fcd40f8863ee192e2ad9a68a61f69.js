var logging = require('./logging');

/**
 * Base error handler, only log stacktrace.
 */
function logErrors(err, req, res, next) {
  console.log(err);
  logging.error({ message: err.message, stacktrace: err.stack });
  next(err);
}

/**
 * Development error handler. Will print stacktrace.
 */
function developmentErrorHandler(err, req, res, next) {
  logging.error(err);
  res.status(err.status || 500);
  res.send({
      message: err.message,
      error: err,
      title: 'error',
      stacktrace: err.stack
  });
};

/**
 * Production error handler. Will print stacktrace.
 */
function productionErrorHandler(err, req, res, next) {
  res.status(err.status || 500);
  res.send({
    message: err.message,
    error: {},
    title: 'error'
  });
};

module.exports = {
  logErrors: logErrors,
  development: developmentErrorHandler,
  production: productionErrorHandler
};