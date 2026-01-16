const express = require('express');
var bodyParser = require('body-parser')
var app = express();

// parse application/json
app.use(bodyParser.json())

app.use(express.static(__dirname));

var port = process.env.PORT || 3000
app.listen(port, function() {
    console.log("To view your app, open this link in your browser: http://localhost:" + port);
});
