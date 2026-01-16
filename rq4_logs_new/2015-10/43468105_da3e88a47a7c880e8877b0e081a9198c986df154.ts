/*
 * GET /
 */
import express = require("express");
import mongoose = require("mongoose");
import db = require("../models/Database");
import vm = require("../viewmodels/home/IndexViewModel");

// GET /
export function index(req: express.Request, res: express.Response) {
    db.Connect();

    var ItemContainer = mongoose.model<db.Schema.IItem>(db.Schema.Item);

    var item = new ItemContainer({
        Name: "Bruno M",
        Description: "God like programmer",
    });

    item.save(err => { console.log(err); });

    ItemContainer.find({}, (err, items) => {
        console.log(items);
    });

    var viewModel = new vm.IndexViewModel();

    res.render("index", viewModel);
};