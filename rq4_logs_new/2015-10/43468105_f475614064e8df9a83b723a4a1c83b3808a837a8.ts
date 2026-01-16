import express = require("express");

import homeController = require("./HomeController");
import userController = require("./UserController");

class RouteConfig {
    static Register(app: express.Application) {
        homeController.Register(app);
        userController.Register(app);
    }
}

export = RouteConfig;