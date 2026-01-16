var http = require('http');
var fs = require('fs');
var path = require('path');
var express = require('express');

var app = express();


app.set('views engine', 'ejs');

app.get('/' ,function(req,res){
    res.send('hi');
    
});
app.get('/home' ,function(req,res){
    
    res.render('home', {person: "hi"});
    
});
console.log("server Start at Port number 5000");
app.listen(5000);

var server = http.createServer(function(re,res){
    console.log('From Website');
    res.writeHead(100,{'Content-Type': 'text/plain'});
    res.end('Http Server');
});

server.listen(5000,"127.0.0.1");