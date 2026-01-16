var express = require('express');
var path = require('path');

var app = express();

var port = process.env.PORT || 1337;

var characters = [
    'Holt',
    'Talon',
    'Brinton',
    'Avicenna',
    'Grigg',
    'Kosomov',
    'Blutock',
    'Marxman'
];

app.use(express.static('client'));

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname + '/client/index.html'));
});

app.get('/characters', function(req, res) {
    res.json(characters);
});

app.listen(port);
console.log('App is listening on port ' + port);