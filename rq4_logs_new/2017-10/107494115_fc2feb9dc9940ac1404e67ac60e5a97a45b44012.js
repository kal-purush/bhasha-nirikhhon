var express    = require("express"),
    app        = express(),
    bodyParser = require("body-parser"),
    mongoose   = require("mongoose");

mongoose.connect("mongodb://localhost/yelp_camp", {useMongoClient: true});
app.use(bodyParser.urlencoded({ extended: true }))
app.set("view engine", "ejs");

// Create the structure of campgrounds in db
var campgroundShema = new mongoose.Schema({
    "name": String,
    "image": String
});

// Create/use "campgrounds" db in mongodb - we refer to is as "Campground" in this file even though it is "campgrounds" in our db.
var Campground = mongoose.model("Campground", campgroundShema);

app.get("/", function(req, res){
   res.render("landingpage"); 
});


// INDEX - Show all campgrounds
app.get("/campgrounds", function(req, res){
    Campground.find({}, function(err, allCampgrounds){
        if(err){
            console.log(err);
        }else{
             res.render('campgrounds', {campgrounds:allCampgrounds});
        }
    });
});

//CREATE - Add new campground to database
app.post('/campgrounds', function(req, res){
    var name = req.body.name;
    var image = req.body.image;
    var newCampground = {name: name, image: image};
    Campground.create(newCampground, function(err, newlyCreated){
        if(err){
            console.log(err);
        }else {
            res.redirect("/campgrounds");
        }
    });
    
});

// NEW - Get form to create new campground
app.get("/campgrounds/new", function(req, res){
    res.render("newCampground");
});

app.listen(process.env.PORT, process.env.IP, function(){
    console.log('server has started');
});

    