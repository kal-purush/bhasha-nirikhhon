"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = void 0;

var _express = _interopRequireDefault(require("express"));

var _bodyParser = require("body-parser");

var _morgan = _interopRequireDefault(require("morgan"));

var _cors = _interopRequireDefault(require("cors"));

var _authRoutes = _interopRequireDefault(require("./routes/auth-routes"));

var _eventRoutes = _interopRequireDefault(require("./routes/event-routes"));

var _config = _interopRequireDefault(require("../database/config"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { "default": obj }; }

/* se importa express, json y morgan */

/* se importan las rutas */
var app = (0, _express["default"])(); // Base de datos

(0, _config["default"])();
/* middlewares */

app.use((0, _morgan["default"])('dev')); //para ver las peticiones que llegan al servidor

app.use((0, _bodyParser.json)()); // para procesar datos en formato json

app.use((0, _cors["default"])()); //Rutas

app.use('/api/auth', _authRoutes["default"]);
app.use('/api/events', _eventRoutes["default"]); // Lectura y parseo del body
//Directorio publico

app.use(_express["default"]["static"]('public'));
var _default = app;
exports["default"] = _default;