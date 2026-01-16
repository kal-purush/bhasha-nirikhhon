"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express = require("express");
const session = require("express-session");
const routes_1 = require("./routes/routes");
const path = require("path");
class App {
    constructor() {
        this.routePrv = new routes_1.Routes();
        this.app = express();
        this.config();
        this.routePrv.routes(this.app);
    }
    config() {
        this.app.use(express.json());
        this.app.use(express.urlencoded({ extended: false }));
        this.app.use(express.static('public'));
        this.app.set('views', path.join(__dirname, '..', '/views'));
        this.app.set('view engine', 'ejs');
        this.app.use(session({
            secret: 'keyboard cat',
            resave: false,
            saveUninitialized: true
        }));
    }
}
exports.default = new App().app;