const http = require('http');
var redis = require("redis");

const hostname = '127.0.0.1';
const port = 8181;
var client = redis.createClient("6379",hostname, null);

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
  res.setHeader('Access-Control-Allow-Credentials', true);
  
  client.hgetall("hash_tag", function (err, object) {
	    var output = "[";
	    var counter = 0;
		try {
		    var items = Object.keys(object).map(function(key) {
		    return [key, object[key]];
		    });

		    items.sort(function(first, second) {
		        return second[1] - first[1];
		    });
		      
		    for (var i = 0; i < items.length; i++){ 
		        output+= `{\"ht\":\"`+items[i][0]+`\", \"count\":\"`+items[i][1]+`\"}`;
		        if (i+1 != items.length) {
		          output += ",";
		        }
		    }
		}catch(err){
			console.log(err);
		}
	    res.write(output+']');
	  	res.end();
  });

	
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});