var express=require("express");
var app=express();
var bodyparser = require("body-parser");
var fileupload = require("express-fileupload");
var flash=require("express-flash");
var session=require("express-session")
var cookieParser=require("cookie-parser");
const compression = require("compression");
app.use(bodyparser());
app.use(compression())
app.use(express.static(__dirname+"/public/"));
app.use(cookieParser())
app.use(fileupload())
app.set("view engine","ejs");
app.use(flash())
app.use(session({secret:"TSS",saveUninitialized:true}))
app.use(require("./controller/default"))
app.listen(process.env.PORT || 7000,function(){ 
    console.log("Server Started!!");
});