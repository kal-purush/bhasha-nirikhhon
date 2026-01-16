var aws = require('aws-sdk');
var crypto = require('crypto');
var ddb = new aws.DynamoDB({region: 'us-west-2'});     

exports.handler = (event, context, callback) => {
    
    
    var apiResponse = {};
    var longUrl = event.headers.url;
    var shortUrl = crypto.createHash('md5').update(longUrl).digest('hex').substring(0,6);
    
    var params = {
        Item: {
            "longUrl": {
                S: longUrl
                }, 
            "shortUrl": {
                S: shortUrl
                } 
            },
            ReturnConsumedCapacity: "TOTAL", 
            TableName: "url-shortner-new"
        };

    var request = ddb.putItem(params);
    
    request.
        on('success', function(response) { 
            var sentUrl = "https://7s8vflux8c.execute-api.us-east-1.amazonaws.com/v1/" + shortUrl;
                var apiResponse = {
            statusCode: 200,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            body: JSON.stringify(sentUrl)
        };
            console.log(apiResponse);
            callback(null, apiResponse);
        }).
        on('error', function(response){
            
            apiResponse = {
                statusCode: 500,
                headers: {},
                body: "There was an error processing your request " + JSON.stringify(response.data)
            }; callback(null, apiResponse);
            
        }).
    send();
};