var express = require('express');
var morgan = require('morgan');
var app = express();
var path = require('path');

app.use(morgan('combined'));

// viewed at http://localhost:8080
app.use("/assets", express.static(path.join(__dirname + '/assets')));
app.use("/src", express.static(path.join(__dirname + '/src')));

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname + '/index.html'));
});

app.listen(8080);