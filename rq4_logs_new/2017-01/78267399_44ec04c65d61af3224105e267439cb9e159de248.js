var express = require('express');
var uaParser = require('ua-parser');
var accepts = require('accepts');

var app = express();
var obj;

app.get('/getinfo',function(req,res){
	var r = uaParser.parse(req.headers['user-agent']);
	console.log(r.os.toString());
	obj = {
		'ipaddress': req.ip,
		'language': accepts(req).languages()[0],
		'software': r.os.toString(),
	}

	console.log(obj);
	res.send(obj);
})

app.listen(process.env.port||3000);