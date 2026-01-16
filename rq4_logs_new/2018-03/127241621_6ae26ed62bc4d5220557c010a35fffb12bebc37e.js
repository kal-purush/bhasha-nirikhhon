var express = require('express');
var bodyParser=require('body-parser');
var app=express();

app.get('/getCurrencyconvertorfactor',function(req,res)
{
    //console.log(req.query);
    //res.send("Hello "+ req.query.name);
    res.json({result:64.72});
});
app.listen(3003);