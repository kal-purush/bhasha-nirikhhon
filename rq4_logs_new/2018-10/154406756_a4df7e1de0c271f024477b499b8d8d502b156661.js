var express = require('express');
const path = require ('path');
var app = express();

app.use (express.static ( path.join (__dirname, 'public')));
app.get ('/', function (req, res){
    res.sendfile(__dirname + '/public/html/index.html')
})

app.listen(3000, function () {
  
    console.log('Example app listening on port 3000!')
})