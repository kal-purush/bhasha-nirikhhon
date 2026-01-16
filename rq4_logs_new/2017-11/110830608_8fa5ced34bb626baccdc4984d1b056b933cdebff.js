'use strict';

var https = require('https');

process.env.DEBUG = 'actions-on-google:*';
const App = require('actions-on-google').DialogflowApp;
const functions = require('firebase-functions');


// a. the action name from the make_name Dialogflow intent
const SMS_ACTION = 'send_sms';
// b. the parameters that are parsed from the make_name intent
const PHONE_NUMBER_ARGUMENT = 'phone-number';
const SMS_TEXT_ARGUMENT = 'sms-text';

//-----------------------------------------

exports.dialogflowFirebaseFulfillment = functions.https.onRequest((request, response) => {
    const app = new App({request, response});
    console.log('Request headers: ' + JSON.stringify(request.headers));
    console.log('Request body: ' + JSON.stringify(request.body));

    // c. The function that send SMS using MM Rest API
    function dialogflowFirebaseFulfillment (app) {
        let mobileNumber = app.getArgument(PHONE_NUMBER_ARGUMENT);
        let smsText = app.getArgument(SMS_TEXT_ARGUMENT);
        
        if(mobileNumber.charAt(0) == "0" && mobileNumber.charAt(1) == "4")
        {
            mobileNumber = mobileNumber.replace(mobileNumber.charAt(0), "61");
        }
        
        console.log("DEBUG sms-text", smsText);
        console.log("DEBUG mobileNumber", mobileNumber);

        /**
        * HOW TO Make an HTTP Call - POST
        */
        // do a POST request
        // create the JSON object
        var jsonObject = JSON.stringify({
            "messages" : [
                {
                    "content": smsText,
                    "destination_number": "+" + mobileNumber,
                    "format": "SMS"
                }
            ]
        });

        console.log("DEBUG request: ", jsonObject);

        // prepare the header
        var postheaders = {
            'Content-Type' : 'application/json',
            'Authorization' : 'Basic MGlhaEprODRaaG9kZFROcGtyMnM6RkVMMDZBV1h1VlIwNXdocWtNbmY2MUZzVHpxa3Fo'
        };

        // the post options
        var optionspost = {
            host : 'api.messagemedia.com',
            port : 443,
            path : '/v1/messages',
            method : 'POST',
            headers : postheaders
        };

        console.info('Options prepared:');
        console.info(optionspost);
        console.info('Do the POST call');

        // do the POST call
        var reqPost = https.request(optionspost, function(res) {
            console.log("statusCode: ", res.statusCode);
            // uncomment it for header details
            // console.log("headers: ", res.headers);

            res.on('data', function(d) {
                console.info('POST result:\n');
                console.info(d);
                console.info('\n\nPOST completed');
            });
        });

        // write the json data
        reqPost.write(jsonObject);
        reqPost.end();
        reqPost.on('error', function(e) {
            console.error(e);
        });
        //-----------------------------------------
    
        var mobileSplit = mobileNumber.split("");
        app.tell('Have sent the SMS to ' + mobileSplit + '! For more info on please go to messagemedia.com.au .');

    }
    // d. build an action map, which maps intent names to functions
    let actionMap = new Map();
    actionMap.set(SMS_ACTION, dialogflowFirebaseFulfillment);


    app.handleRequest(actionMap);
});