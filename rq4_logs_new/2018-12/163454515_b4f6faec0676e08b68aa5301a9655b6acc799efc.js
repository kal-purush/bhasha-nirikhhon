var express=require("express");
var app=express();
var parser=require("body-parser");
var mongoose=require("mongoose");
var flash=require("connect-flash");
var methodOverride=require("method-override");
var Campground=require("./models/campground");
var seedDB=require("./seeds");
var Comments=require("./models/comment")
var passport=require("passport");
var localStrategy=require("passport-local");
var User=require("./models/user");

// Requiring routes
var commentRoutes=require("./routes/comments");
var campgroundRoutes=require("./routes/campgrounds");
var indexRoutes=require("./routes/index");


//seedDB();
app.listen(3000,function(req,res){
  console.log("Serv has started");
});


mongoose.connect("mongodb://localhost/yelp_camp",{useNewUrlParser:true});
//mongodb://Himesh619:himesh@1998@ds145923.mlab.com:45923/yelpcamp_new
app.use(parser.urlencoded({extended:true}));
app.set("view engine","ejs");
app.use(express.static(__dirname+"/public"));
app.use(methodOverride("_method"));
app.use(flash());

//Passport Configuration
app.use(require("express-session")({
  secret:"qwerty",
  resave:false,
  saveUninitialized:false
}));

app.use(passport.initialize());
app.use(passport.session());
passport.use(new localStrategy(User.authenticate()));
passport.serializeUser(User.serializeUser());
passport.deserializeUser(User.deserializeUser());

app.use(function(req,res,next){
  res.locals.currentUser=req.user;
  res.locals.error=req.flash("error");
  res.locals.success=req.flash("success");
  next();
});

app.use("/",indexRoutes);
app.use("/campgrounds/:id/comments",commentRoutes);
app.use("/campgrounds",campgroundRoutes);