var express = require("express")  // requiring express, though xps is built in we dont us ./
var chalk = require("chalk")  // requiring chalk for colouring the console.log messages for developers use
var bodyparser= require("body-parser")
var path = require('path');
var app = new express();  // new keyword is optional and app is the object
var nav=[{link:'/books', title:'Books'},{link:'/authors',title:'Authors'},{link:'/books/addBooks',title:'Add Books'}];
var authorsRouter= require('./src/routes/authorsRouter')(nav);
var booksRouter= require('./src/routes/booksRouter')(nav);
app.use(express.static(path.join(__dirname, "/public")));
app.use(bodyparser.urlencoded({extended: true}));
app.use('/books',booksRouter);
app.use('/authors',authorsRouter);
app.set('views', './src/views');
app.set('view engine','ejs');


app.get("/", function(req,res){  //get,all,post can be used, these all are requists
    //res.send("HELLO world")  // CALLBACK / mean up to the root

   // res.sendFile(path.join(__dirname+"/src/views/index.html"));
   res.render(
       'index.ejs',
       {nav,title:"Library"})
});      // telling app to get something

           
//app.get("/books", function(req,res){
       /////// ejs is installed to 


app.get("/authors", function(req,res){
    res.render("authors.ejs",{nav,title:"Authors"});   /////// ejs is installed to 
});
app.listen(3000,function(){
    console.log('listening to port '+chalk.green('3000'));
});  // mentioning the port number 3000