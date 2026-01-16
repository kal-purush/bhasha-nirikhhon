var express = require('express');
var app = express();

var pg = require("pg");
const connectionString = "postgres://sbrenchley:sbrenchley_pass@localhost:5432/twitter";

app.set('port', (process.env.PORT || 5000));
app.use(express.static(__dirname + '/public'));

app.listen(app.get('port'), function() {
  console.log('Node app is running on port', app.get('port'));
});