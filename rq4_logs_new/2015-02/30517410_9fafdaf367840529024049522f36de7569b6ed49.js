var fs 			= require('fs');
var http 		= require('http');
var https 		= require('https');
var url 		= require('url');
var express 	= require('express');
var oriento 	= require('oriento');
var bodyParser	= require('body-parser');
var app 		= express();
app.use(bodyParser());
app.set('view engine','ejs');
var ROOTDIR = "html/";

var server 	= oriento({
    host 	: 'localhost',
    port 	: 2424,
    username: 'root',
    password: 'password'
});

var db = server.use({
	name  	: 'testDB',
	username: 'root',
	password: 'password'
});

app.get("/",function(req,res){
	fs.readFile(ROOTDIR+"home.html",function(err,data){
		if(err){
			res.writeHead(404);
			res.end(JSON.stringify(err));
			return;
		}
		res.writeHead(200)
		res.end(data);
	});
});

app.post("/",function(req,res){
	var validEmail = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
	if(validEmail.test(req.body.email_address)) {
		db.query('insert into samplePerson (Email,name,password,status) values (:email,:name,:password,:status)',
	  		{
	    		params: {
	      			email 	: req.body.email_address,
	      			name 	: 'guest_'+req.body.email_address,
	      			password: 'default',
	      			status 	: 'subscribed'
	    		}
	  		}
		).then(function (response){
  			res.send("Thank You!! You are been subscribed");
		});
	} else {
		res.send("Please enter valid email address");
	}
});

app.listen(8080);

/*db.query('select from samplePerson where name=:name', {
  params: {
    name: 'Prasad'
  },
  limit: 1
}).then(function (results){
  console.log(results);
});*/

/*server.list().then(function (dbs) {
	console.log('There are ' + dbs.length + ' databases on the server.');
	
	var firstDB = dbs[1];

    var orientDB = server.use({
        name: firstDB.name,
        username: firstDB.username,
        password: firstDB.password
    });

    console.log('Using database: ' + orientDB.name + " : " + orientDB.type);
}).catch(function (err) {
	console.log(err);
});*/