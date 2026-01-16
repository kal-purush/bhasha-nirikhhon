"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var express_1 = __importDefault(require("express"));
var index_1 = __importDefault(require("./routes/index"));
// create application object
var app = (0, express_1.default)();
// process.env.PORT if we are running in production || if we run locally
// const port = process.env.PORT || 3000;
var port = 3000;
// setting the root path to the root route as a middleware
app.use('/api', index_1.default);
// listen to the server
app.listen(port, function () {
    console.log("server started at localhost:".concat(port));
});
exports.default = app;