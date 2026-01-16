//Express
var express = require('express');
var app = express();
var mongoose = require('mongoose');

var Schema = mongoose.Schema
var schema = new Schema({ name: String })
var Model = mongoose.model("ModelName", schema)

mongoose.connect(process.env.MONGOLAB_JADE_URI || "mongodb://localhost/herokutest");

var bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());

app.use("/node_modules", express.static(__dirname + '/node_modules'));

app.get('/', function(req, res){
	res.sendFile(__dirname + "/index.html");
});

app.post('/test', function(req,res){
	var m = new Model(req.body)
	console.log("saved", m)
})

app.listen(process.env.port || 1337)


