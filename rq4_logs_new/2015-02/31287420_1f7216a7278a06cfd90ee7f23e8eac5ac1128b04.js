var express = require('express');
var bodyParser = require('body-parser');
var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var app = express();
app.set('view engine', 'jade');
app.set('views', __dirname + '/views');
app.use(express.static(__dirname + '/public'));
app.use(bodyParser());

mongoose.connect('mongodb://localhost/coffeeroast');

var personSchema = new Schema ({
  name: String,
  bio: String,
  skills: [String],
  years: Number,
  why: String
});

var Person = mongoose.model('Person', personSchema);

app.get('/', function(req, res) {
	res.render('index');
});

// displays a list of applicants
app.get('/applicants', function(req, res){
  Person.find({}, function(err, data){
    res.render('applicants', {theData: data});
    // console.log('data::::', data, 'err::::', err)
  });
});

// creates and applicant
app.post('/applicant', function(req, res){
	var aPerson = new Person ({
    name: req.body.name,
    bio: req.body.bio,
    skills: req.body.skills,
    years: req.body.years,
    why: req.body.why
  });

  aPerson.save(function(err){
    if (err){throw err}
    else{console.log('SAVED A PERSON:::::::::::::::::::')}
  });


	res.redirect('applicants');
});

var server = app.listen(8441, function() {
	console.log('Express server listening on port ' + server.address().port);
});

// console.log(Person);

// module.exports = mongoose.model('Person', personSchema);