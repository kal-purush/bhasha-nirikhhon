var express               = require("express");
    app                   = express(),
    bodyParser            = require("body-parser"),
    passport              = require("passport"),
    LocalStrategy         = require("passport-local"),
    passportLocalMongoose = require("passport-local-mongoose"),
    mongoose              = require("mongoose"),
    expressSession        = require("express-session"),
    port                  = process.env.PORT || 3823;

 

    // mongoose.connect("mongodb://localhost/auth_demo",{
mongoose.connect("mongodb://jalak:123456789jalak@ds155714.mlab.com:55714/user",{
      useMongoClient:true
    })
    mongoose.Promise = global.Promise;

app.use(bodyParser.urlencoded({extended:true}));
app.use(express.static(__dirname+"/public"));
var User                  = require("./models/user"),
    userRoute             = require('./routes/userRoute');
    searchRoute           = require('./routes/searchRoute');
//=========================================================
//=============Initializing Session & Passport=============
//=========================================================
app.use(expressSession({
  secret:"Rusty is the best dog in the world", // is used to enco-deco information in the session
  resave:false,
  saveUninitialized:false
}))
app.use(passport.initialize()); // always needed to use the passport methods
app.use(passport.session());
//===========================================================
//== This function will run in all the routes as middleware =
//== The currentUser will be available in all the templates =
//===========================================================
app.use(function (req,res,next) {
  res.locals.currentUser = req.user;
  next();
});
//=========================================================
//=============User routes=======================
//=========================================================
app.use(userRoute);
//=========================================================
//=============User routes=======================
//=========================================================
app.use(searchRoute);
//=========================================================
//============= Home Routes===============================
//=========================================================
app.get("/",isLoggedIn,function(req,res){
  res.render("searchResults.ejs");
})

//=========================================================
//=============Authenticating function=====================
//=========================================================
// authenticating if user is loggedin or not
function isLoggedIn(req,res,next) {
  if (req.isAuthenticated()){
    console.log("user is logged in");
    return next();
  }
  console.log("user is not logged in");
    res.redirect("/login");
}
//=========================================================
//=============Logout route================================
//=========================================================
app.get("/logout",function(req,res){
  req.logout();
  res.redirect("/");
})
//=========================================================
//============= To create Admin ===========================
//=========================================================

User.register(new User({username:"jalak",role:"admin"}),"pass",function(err,user){})

//  testing
// app.get("/searchResult", (req, res) => {
//   res.send("hello");
// });
//  testing
//=========================================================
//=============Listening to port===========================
//=========================================================
app.listen(port,function(){
  console.log(`server connected at: ${port}`);
})