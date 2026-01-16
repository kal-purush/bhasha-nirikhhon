var express      = require("express"),
methodOverride   = require("method-override"),
expressSanitizer = require("express-sanitizer")
bodyparser       = require("body-parser"),
mongoose         = require("mongoose"),
flash            = require("connect-flash");
app              = express();

require("dotenv").config({ path: "./.env" });

mongoose.connect(process.env.Mongodb_URL, { useNewUrlParser: true , useUnifiedTopology: true , useFindAndModify: false });
app.set("view engine", "ejs");
app.use(express.static("public"));
app.use(bodyparser.urlencoded({extended: true}));
app.use(methodOverride("_method"));
app.use(expressSanitizer());

// Express session required
app.use(require("express-session")({
    secret: "Aything with the designer",
    resave: false,
    saveUninitialized: false,
}));


// Enable the app to use the Flash
app.use(flash());

// Local Storage variables
app.use(function(req, res, next){
  res.locals.error = req.flash("error");
  res.locals.success = req.flash("success");
  next();
})

var indexRoutes = require("./routes/index.js");

app.use("/", indexRoutes);

app.listen(80, function(){
    console.log("The App has started");
})