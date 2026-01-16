/*
 * GET /
 */
import express = require("express");
import mongoose = require("mongoose");
import db = require("../models/Database");
import vm = require("../viewmodels/home/IndexViewModel");

import baseController = require("./BaseController");

class HomeController extends baseController.BaseController {

    // GET /
    static Index(req: express.Request, res: express.Response) {
        super.run(req, res);

        res.render("home/index", new vm.IndexViewModel());
    }
}

export = HomeController;