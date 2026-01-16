/// <reference path="./typings/tsd.d.ts" />

import express = require('express');   //Importing express from modules

let app = express();   //set Variable app to express function.

//setting the view engine to ejs

app.set('view engine', 'ejs');

//use res.render to load up an ejs to view file

//index page

app.get('/', function(req: express.Request, res: express.Response){
	
	let scores = [
	{name: "HTML", percentage: '75%'},	
	{name: "CSS", percentage: '60%'},
	{name: 'Express Test 1', percentage: '67%'}
		
	];
	let tagline = "There have been some of my Saylani test records in the list.";
	
	res.render('pages/index',{
		scores: scores,
		tagline: tagline
	});
});

//about page

app.get('/about', function(req: express.Request, res: express.Response){
      //	res.end("hello world");      //just trying to know my is working fine or not?
	res.render('pages/about');

});

//port to listing app
app.listen(80, ()=>{
	console.log('Sever is listing to port 80...');
});