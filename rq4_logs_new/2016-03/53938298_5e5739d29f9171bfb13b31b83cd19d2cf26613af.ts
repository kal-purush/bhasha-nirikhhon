/// <reference path="./typings/main.d.ts" />

import express = require("express");

var router = express.Router();

router.get('/', function getIndex(req, res, next) {
  res.json({});
});

router.get('/:short', function redirectUrl(req, res, next) {
  res.redirect('https://google.com');
});

router.post('/create', function createShortUrl(req, res, next) {
  res.json({ status: "ok" });
})

export = router;
