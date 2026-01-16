'use strict'
var express = require('express');
var router = express.Router();
var mongoose = require('mongoose');
var earthquakeModel = mongoose.model('Earthquake')

module.exports = router

let getEarthquakes = function (req, res, next) {
  earthquakeModel.find(function (err, earths) {
    if (err) {
      console.error(err);
    }
    res.status(200).send(earths);
  });
};

router.get('/', getEarthquakes)