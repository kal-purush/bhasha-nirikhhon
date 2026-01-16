var express=require('express');
var app = express();
var fs = require('fs')
app.set('view engine','ejs');

app.get('/pushState1.html',function(req,res){
	fs.readFile('pushState1.html','utf8', function (err,data) {
  if (err) {
    return console.log(err);
  }
  res.send(data);
});
});

app.get('/pushState2.html',function(req,res){
	fs.readFile('pushState2.html','utf8', function (err,data) {
  if (err) {
    return console.log(err);
  }
  res.send(data);
});
});


app.listen(8080);
console.log('connect');