//jshint esversion:6
require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
var lodash_ = require('lodash');
const ejs = require("ejs");
const mongoose = require("mongoose");
const session = require("express-session");
const passport = require("passport");
const passportLocalMongoose = require("passport-local-mongoose");

const homeStartingContent = "This is the Home Page of your Daily Journal. You can see all your record listed below after adding a new content. Click the read more button next to each post for expanding it to a new page. For adding a new post press the compose button on the navigation bar. Happy writing!";

const app = express();

app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static("public"));

app.use(session({
  secret: process.env.SECRETKEY,
  resave: false,
  saveUninitialized: false
}));

app.use(passport.initialize());
app.use(passport.session());

let blogs=[];

mongoose.connect(process.env.MONGOURL);

const blogSchema = {
  username: String,
  title: String,
  content: String 
};

const userSchema = new mongoose.Schema({
  username: String,
  password: String
});

userSchema.plugin(passportLocalMongoose);

const Blog = mongoose.model("Blog", blogSchema);
const User = new mongoose.model("User",userSchema);

passport.use(User.createStrategy());

passport.serializeUser(User.serializeUser());
passport.deserializeUser(User.deserializeUser());


// ========================  GET REQUESTS  ===========================

app.get("/",function(req,res){
  if(req.isAuthenticated()){
    Blog.find({username: currentUsername},function(err,blogs){
      res.render("home",{
          homeStartingContent: homeStartingContent,
          blogs: blogs
      });
    }); 
  }
  else 
    res.render("login");
});


app.get("/register",function(req,res){
  res.render("register");
});


app.get("/login",function(req,res){
  res.render("login");
});


app.get("/home",function(req,res){
  if(req.isAuthenticated()){
    Blog.find({username: currentUsername},function(err,blogs){
      res.render("home",{
          homeStartingContent: homeStartingContent,
          blogs: blogs
      }); 
    });
  }
  else{
    res.redirect("/login");
  }
});


app.get("/compose",function(req,res){
  if(req.isAuthenticated()){
    res.render("compose");
  }else{
      res.redirect("/login");
  }
});


app.get("/logout",function(req,res){
  req.logout(function(err){
      if(!err)
      {
        res.redirect("/");
      }
  });
});


// ==========================  POST REQUESTS  ============================

var currentUsername;

app.post("/register",function(req,res){
    
  User.register({username: req.body.username}, req.body.password, function(err,user){
      if(err){
          console.log(err);
          res.redirect("/register");
      }else{
          passport.authenticate("local")(req,res,function(){
              currentUsername = req.body.username;
              res.redirect("/home");
          });
      }
  });
});


app.post("/login",function(req,res){
  
  const user = new User({
      username: req.body.username,
      password: req.body.password  
  });

  req.login(user,function(err){
      if(err){
          console.log(err);
      }else{
          passport.authenticate("local")(req,res,function(){
              currentUsername = req.body.username;
              res.redirect("/home");
          });
      }
  });
});


app.post("/compose",function(req,res){

  const blog = new Blog({
    username: currentUsername,
    title: req.body.blogTitle,
    content: req.body.blogContent 
  })

  blog.save(function(err){
    if(!err){
      res.redirect("/home");
    }
  });
});


app.get("/post/:random",function(req,res){
  const blogId = req.params.random; //e.g. ..../post/day-1  so blogId

  Blog.findOne({_id: blogId},function(err,blog){
    res.render("post",{
      title: blog.title,
      content: blog.content
    });
  });
});


app.post("/delete", function(req,res){

  const idDelete= req.body.button;
  Blog.findByIdAndRemove(idDelete,function(err){
  
    if(!err){
      console.log("successfully deleted");
    }
    res.redirect("/home");
  });
});



app.listen(3000, function() {
  console.log("Server started on port 3000");
});