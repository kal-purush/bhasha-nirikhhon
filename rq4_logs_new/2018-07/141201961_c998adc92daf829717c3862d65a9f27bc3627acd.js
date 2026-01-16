

var redis = require("redis");  // import
var client = redis.createClient();

client.get('party',function(err,value) {
    console.log(value); // => 'bar'
});

client.quit();