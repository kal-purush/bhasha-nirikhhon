// 23733235
const fs = require('fs');
const http = require('http');
const https = require('https');

const credentials = require("./auth/credentials.json");
let api_key = credentials['API-Key']; 
const port = 3000;

const server = http.createServer();
server.on("request", request_handler);
server.on("listening", listen_handler);
server.listen(port);

function listen_handler(){
	console.log(`Now Listening on Port ${port}`);
}

function request_handler(req, res){
    console.log(req.url);

    if(req.url === "/"){
        const form = fs.createReadStream("html/index.html");
		res.writeHead(200, {"Content-Type": "text/html"});
		form.pipe(res);
    }

    else if(req.url.startsWith("/search")){
        const user_input = new URL(req.url, `https://${req.headers.host}`).searchParams;
        console.log(user_input);
        const ip = user_input.get('ip');
        if(ip == null || ip == ""){
            res.writeHead(404, {"Content-Type": "text/html"});
            res.end("<h1>Missing Input</h1>");        
        }
        else{
            const ip_api = https.request(`https://ipgeolocation.abstractapi.com/v1/?api_key=${api_key}&ip_address=${ip}`);

            ip_api.on("response" , ip_res => process_stream(ip_res, parse_results, res));
			ip_api.end();
        } 
    }

    else{
    res.writeHead(404, {"Content-Type": "text/html"});
    res.end("<h1>404, Not Found</h1>");    
    }

}

function process_stream (stream, callback , ...args){
	let body = "";
	stream.on("data", chunk => body += chunk);
	stream.on("end", () => callback(body, ...args));
}

function parse_results(data, res){
    const lookup = JSON.parse(data);
	if(typeof lookup.city !== 'undefined'){
		let longitude = lookup.longitude;
		let latitude = lookup.latitude;
		weather_api_call(res, longitude, latitude);
    }
	else{
		let results = "<h1>No Results Found!</h1>";
		res.writeHead(404, {"Content-Type": "text/html"});
    	res.end(results);

	}
}

function weather_api_call(res, longitude, latitude, city){
	const weather_api = https.request(`https://api.open-meteo.com/v1/forecast?latitude=${latitude}&longitude=${longitude}&current_weather=true`);
    weather_api.on("response" , weather_res => process_weather_stream(weather_res, parse_weather_results, res));
	weather_api.end();
}


function process_weather_stream(stream, callback , ...args){
	let body = "";
	stream.on("data", chunk => body += chunk);
	stream.on("end", () => callback(body, ...args));
}

function parse_weather_results(data, res){

	const weather_lookup = JSON.parse(data);
	let temperature = ((weather_lookup.current_weather.temperature * 9/5) + 32).toFixed();
	let weather_code = weather_lookup.current_weather.weathercode;

	let results = 
	`
	<style>
	.boxed {
		border: 1px solid black ;
	}
	</style>
	<div class="boxed">
	<center><h1>Weather:</h1>
	<h2>The temperature outside the location of the current IP adress is: ${temperature} degrees Fahrenheit!</h2>
	<h2>Weather code: ${weather_code}</h2>
	</div>

	<center>
	<p>Weather Code | Description</p>
	<p>  0	| Clear sky</p>
	<p>  1, 2, 3	| Mainly clear, partly cloudy, and overcast</p>
	<p>45, 48	| Fog and depositing rime fog</p>
	<p>51, 53, 55	| Drizzle: Light, moderate, and dense intensity</p>
	<p>56, 57	| Freezing Drizzle: Light and dense intensity</p>
	<p>61, 63, 65	| Rain: Slight, moderate and heavy intensity</p>
	<p>66, 67	| Freezing Rain: Light and heavy intensity</p>
	<p>71, 73, 75	| Snow fall: Slight, moderate, and heavy intensity</p>
	<p>77		| Snow grains</p>
	<p>80, 81, 82	| Rain showers: Slight, moderate, and violent</p>
	<p>85, 86	| Snow showers slight and heavy</p>
	<p>95 *	| Thunderstorm: Slight or moderate</p>
	<p>96, 99 *	| Thunderstorm with slight and heavy hail</p>
	`
    res.writeHead(200, {"Content-Type": "text/html"});
	res.end(results);
}