var express = require('express');
var path = require('path');

var app = express();

// set static path middleware
app.use(express.static(path.join(__dirname, 'public')))

app.listen('3000', function() {
    console.log('Expres server is running on port 3000!');
});