var express = require('express');
const puppeteer = require('puppeteer');
var app = express();
const path = require('path');
var screenshotModule = require('./routes/screenshotModule');

global.POWERBI_URL;

POWERBI_URL = path.join(__dirname + '/public/html/powerbi.html') 

app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname + '/public/html/powerbi.html'));
  screenshotModule.takeScreenShot();
});


app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});