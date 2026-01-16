var bodyParser = require('body-parser');
var mongoose   = require('mongoose');
var express    = require('express');
var app        = express();

mongoose.connect('mongodb://localhost/restful_blog_app');
app.set('view engine', 'ejs');
app.use(express.static('public'));
app.use(bodyParser.urlencoded({extended: true}));

var blogSchema = new mongoose.Schema({
  title: String,
  image: String,
  body: String,
  Created: {type: Date, default: Date.now}
});

var Blog = mongoose.model('Blog', blogSchema);

/****************************************
            // ROUTES //
****************************************/

app.get('/blogs', function(req, res) {
  res.render('index');
});

app.listen(27017, function() {
  console.log("Server has started..");
});