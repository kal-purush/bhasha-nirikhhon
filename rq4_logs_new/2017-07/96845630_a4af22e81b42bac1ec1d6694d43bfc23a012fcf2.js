
var http = require('http');
var https = require('https');
var express = require('express');

var app = express();

app.use('/images', express.static('images'));
app.use('/css', express.static('css'));
app.use('/fonts', express.static('fonts'));
app.use('/js', express.static('js'));

app.get('/', function(req, res){
	res.sendFile('index.html', {'root':__dirname});
});

var spbUrl = 'https://hackerearth.0x10.info/api/learning-paths?type=json&query=list_paths';

/*
// uncomment this to get data directly from the above url
app.get('/data', function(req, res){
	https.get(spbUrl, function(webResp){
		var respBody = '';
		webResp.on('data', function(data){
			respBody += data;
		});
		webResp.on('end',function(){
			res.set('Content-Type', 'application/json');
			res.send(respBody);
		});
	});
});
*/

// comment this if the above logic is uncommented
app.get('/data', function(req, res){
	res.set('Content-Type', 'application/json');
	res.send({"paths":[{"id":"1","name":"User Experience Design","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/ux_design_thumb.png","tags":"design research, UI frameworks, wireframes, user centric approach","learner":"26,093","hours":"131+","description":"This Learning Path is a curriculum of UX Design courses, videos and resources from across the internet, organized into a logical sequence that a beginner can follow.","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/user-experience-design\/learn\/"},{"id":"2","name":"MBA Essentials","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/mba_thumb.png","tags":"finance, accounting, opertions, strategy","learner":"15,566","hours":"505+","description":"Learn core business skills and how to apply them professionally and in your personal life","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/mba\/learn\/"},{"id":"3","name":"Android App Dev","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/android_thumb_hHLP9dL.png","tags":"material design, sensors, maps, location service, studio","learner":"22,887","hours":"91+","description":"Learn android programming by building apps from scratch. Learn how to design and build Android apps and take your ideas to millions of people.","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/android\/learn\/"},{"id":"4","name":"Social Entrepreneurship","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/social_ent_thumb.png","tags":"design solution, business plan, failing fast, learning quick","learner":"1,715","hours":"17+","description":"A robust introduction to Social Entrepreneurship, Learn and build a roadmap to launch your very own social venture","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/social-entrepreneurship\/learn\/"},{"id":"5","name":"Apply to Y Combinator","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/y_comb_thumb.png","tags":"startups, hackernews, ycombinator,","learner":"1,658","hours":"3+","description":"A set of resources from YC partners and alumni to help you turn in a strong YC application. This course is collection of Gautam Tambay, Founder, Springboard.","sign_up":"https:\/\/www.springboard.com\/learning-paths\/apply-to-Ycombinator\/learn"},{"id":"6","name":"Data Analysis","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/data_analysis_thumb.png","tags":"processing, wrangling, visualizations, prediction, analysys","learner":"34,068","hours":"310+","description":"Learn statistics, data wrangling and visualization with this free curriculum By an Airbnb\/MIT alum. Learn how to manipulate and analyze data better with this free online curriculum","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/data-analysis\/learn\/"},{"id":"7","name":"Backend: Py\/Django","image":"https:\/\/d1v7bd3b0sjvlo.cloudfront.net\/uploads\/learning_path\/thumb\/web2.png","tags":"python, django, backend","learner":"16,830","hours":"74+","description":"Learn how to build websites and web-apps from scratch using Python and Django. Build and deploy fully functional web applications. Become a full-stack developer with the help of one of the founder Paul.","sign_up":"https:\/\/www.springboard.com\/accounts\/google\/login\/?process=login&next=\/learning-paths\/web-development-python-django\/learn\/"}],"quote_max":"100000","quote_available":"35364"});
});


app.listen(8000, function(){
	console.log('Springboard - Learning Hub ::: Web App running on port 8000');
});




