require("dotenv").config();
var debug = require("debug")("parts-detect");

var express = require("express"),
  path = require("path"),
  logger = require("morgan"),
  bodyParser = require("body-parser"),
  api = require("./routes/api"),
  services = require("./services"),
  app = express();

// view engine setup
app.use(bodyParser.json({ limit: "1mb" }));
app.use(bodyParser.urlencoded({ extended: false }));

app.use("/", api);

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  console.log("~~~~~~~~~~~~~~~~~~~~~`err", err);

  res.status(err.status || 500);
  res.send({
    message: err.message,
    error: {
      code: err.code
    }
  });
});

app.set("port", process.env.SERVERPORT);

var server = app.listen(app.get("port"), function() {
  console.log("Express server listening on port " + server.address().port);
  debug("Express server listening on port " + server.address().port);
});

module.exports = app;