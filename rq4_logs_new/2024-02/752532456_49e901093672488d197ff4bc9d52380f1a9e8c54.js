const { logEvent } = require("./logEvents");

const errorHandler = (err, req, res, next) => {
  logEvent(`${err.name}:${err.message}`);
  console.error(err.stack);
  res.status(500).send(err.message);
};

module.exports = errorHandler;