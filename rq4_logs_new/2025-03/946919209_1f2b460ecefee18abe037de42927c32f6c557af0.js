const express = require('express');

const app = express();

app.get("/",function(req,res){
    res.send("heloo this is home page");
})

app.listen('3000',function(req,res){
    console.log("Server start on 30000");
})