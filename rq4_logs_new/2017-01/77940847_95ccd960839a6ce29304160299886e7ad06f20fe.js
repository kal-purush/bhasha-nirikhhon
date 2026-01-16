var express = require('express');
var app = express();

app.get("/:url", function(req, res){
    var url = req.params.url
})