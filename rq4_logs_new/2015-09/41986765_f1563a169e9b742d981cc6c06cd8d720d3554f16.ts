import * as http from "../index";

http.createClient(8080, "https://github.com");

http.createServer((req, res) => {
	req.on('connect', blah => {
		
	});
	res.end(new Buffer(""), _ => {
		
	});
});