console.log('Loading function');
var AWS = require("aws-sdk");
AWS.config.region = 'us-west-2';

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    event.Records.forEach(function(record) {
        console.log('DynamoDB Record: %j', record.dynamodb);
    });

    var sns = new AWS.SNS();
    eventText = "DB UPDATED";
    var params = {
        Message: eventText, 
        Subject: "Test SNS From Lambda",
        TopicArn: "arn:aws:sns:us-west-2:979829065790:AirBagInfo"
    };
    sns.publish(params, function(err, data) {
        if (err) {
            console.log(err.stack);
            return;
        }
        console.log('SNS topic published.');
        console.log(data);
        context.done(null, 'Function Finished!');  
    });
};