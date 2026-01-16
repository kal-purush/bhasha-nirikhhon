// This loads the environment variables from the .env file
require('dotenv-extended').load();

var builder = require('botbuilder');
var restify = require('restify');
var Store = require('./store');
var spellService = require('./spell-service');
var ssml = require('./ssml');
// var Curl = require( 'node-libcurl' ).Curl;
var twitterAPI = require('./twitterAPI');
const googleTrends = require('google-trends-api');

// Setup Restify Server
var server = restify.createServer();
server.listen(process.env.port || process.env.PORT || 3978, function () {
    console.log('%s listening to %s', server.name, server.url);
});
// Create connector and listen for messages
var connector = new builder.ChatConnector({
    appId: process.env.MICROSOFT_APP_ID,
    appPassword: process.env.MICROSOFT_APP_PASSWORD
});
server.post('/api/messages', connector.listen());

var bot = new builder.UniversalBot(connector, function (session) {
    session.send('Sorry, I did not understand \'%s\'. Type \'help\' if you need assistance.', session.message.text);
});

// You can provide your own model by specifing the 'LUIS_MODEL_URL' environment variable
// This Url can be obtained by uploading or creating your model from the LUIS portal: https://www.luis.ai/
var recognizer = new builder.LuisRecognizer(process.env.LUIS_MODEL_URL);
bot.recognizer(recognizer);


bot.dialog('Search', function (session,args) {
    var entities = [];
    args.intent.entities.forEach(function(element){
        entities.push(element.type);
    });

    var maleEntity = builder.EntityRecognizer.findEntity(args.intent.entities, 'male');
    var femaleEntity = builder.EntityRecognizer.findEntity(args.intent.entities, 'female');

    var targetURL = null;

    var weRetailCategories_deleteThis = {
        eq:[
            'biking','hiking','running','surfing','water-sports','winter-sports'
        ]
        ,me:[
            'coats','footwear','gloves','pants','scarfs','shirts','shorts'
        ]
        ,wo:[
            'coats','gloves','pants','shirts','shorts'
        ]
    };

    var weRetailCategories = {
        surfing:{
            category : 'equipment',
            isGenderSpecific : false,
        },
        'water-sports':{
            category : 'equipment',
            isGenderSpecific : false,
        },
        coats:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:true,
        },
        footwear:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:false,
        },
        gloves:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:true,
        },
        pants:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:true,
        },
        scarfs:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:false,
        },
        shirts:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:true,
        },
        shorts:{
            category : null,
            isGenderSpecific : true,
            male:true,
            female:true,
        },
    };

    for (i=0;i<entities.length && !targetURL;i++){
        if( weRetailCategories[entities[i]] ){
            if (weRetailCategories[entities[i]].category){
                targetURL = 'products/'+weRetailCategories[entities[i]].category+'/'+entities[i];
            }
            else{
                if (femaleEntity && weRetailCategories[entities[i]].female){
                    targetURL = 'products/women/'+entities[i];
                }
                else{
                    targetURL = 'products/men/'+entities[i];
                }
            }
        }
    }
    if (!targetURL){
        if (maleEntity){
            targetURL = 'men';
        }
        else if(femaleEntity){
            targetURL = 'women';
        }
        else{
            targetURL = 'products';
        }
    }
    targetURL = '/content/we-retail/us/en/' + targetURL + '.html';

    // if belong to equipment, go to the target directly
    // for (i=0;i<entities.length && !targetURL;i++){
    //     if (weRetailCategories.eq.includes(entities[i])){
    //         targetURL = 'equipment/'+entities[i];
    //     }
    // }

    // if not belonging to the equipment
        
    var resultObj = {
        interest : entities,
        targetURL: targetURL,
        entities : entities,
        maleEntity : maleEntity,
        femaleEntity : femaleEntity,
        products :[
            {
                "name": "SSGN8"
            },
            {
                "name": "iPhone X"
            }
        ]
    };

    var theTwitterAPI = new twitterAPI(
        "45MrbuMLUpQASjzj28LbXUPE3"
        ,"su3vBrcMXrpFX1uYCgARANOXPDBTe2CmyN5S04h5TQg6TArnAy"
        ,"77930247-2valpma0KuYMWkCqKuSnxbs1APi7VCkLX9sF8Fftp"
        ,"7sJC0gQGaZWhluJJ8azwJL0vcPkBioQUtCYlMSTs73hT3"
    );
    var callback = function(result){
        resultObj.twitterAPIResult = JSON.parse(result);
        session.endDialog(JSON.stringify(resultObj));
    };
    var errorCallback = function(code,errorMessage){
        resultObj.twitterAPIResult = code+' '+errorMessage;
        session.endDialog(JSON.stringify(resultObj));
    };
    theTwitterAPI.fire(
        "https://api.twitter.com/1.1/trends/place.json",
        "GET",
        {
            id:1,
        },
        callback,
        errorCallback
    );

    




    // ============== test google-trends-api =================================================
    // googleTrends.interestOverTime({keyword: 'Women\'s march'})
    // .then(function(results){
    //   resultObj.googleTrends = results;
    //   session.endDialog(JSON.stringify(resultObj));
    // })
    // .catch(function(err){
    //   session.endDialog(JSON.stringify(resultObj));
    // });
    // ============== end test google-trends-api =================================================


    // var sidesEntity = builder.EntityRecognizer.findEntity(args.intent.entities, 'Sides');
    // if (sidesEntity){
    //     game.sides = sidesEntity.entity;
    // }
    // var countEntity = builder.EntityRecognizer.findEntity(args.intent.entities, 'Count');
    // if (countEntity){
    //     game.count = countEntity.entity;
    // }

    // if (sidesEntity) {
    //     // city entity detected, continue to next step
    //     next({ response: sidesEntity.entity });
    // } else {
    //     // no entities detected, ask user for a destination
    //     builder.Prompts.text(session, 'Please enter the number of sides of the dice to be rolled.', { 
    //         speak: speak(session, 'choose_number_of_sides') 
    //     });
    // }

    // session.endDialog(JSON.stringify(resultObj));
}).triggerAction({
    matches: 'Search'
});
    

function camelCaseToNormal(inputString){
    return inputString.replace(/([A-Z])/g, ' $1');
}


    // ============== test node-libcurl =================================================
    // var curl = new Curl();
    // curl.setOpt( 'URL', 
    //     'https://trends.google.com/trends/api/widgetdata/relatedsearches?hl=en-US&tz=-420&req=%7B%22restriction%22:%7B%22geo%22:%7B%22country%22:%22TH%22%7D,%22time%22:%222017-10-07+2017-11-07%22%7D,%22keywordType%22:%22ENTITY%22,%22metric%22:%5B%22TOP%22,%22RISING%22%5D,%22trendinessSettings%22:%7B%22compareTime%22:%222017-09-05+2017-10-06%22%7D,%22requestOptions%22:%7B%22property%22:%22%22,%22backend%22:%22IZG%22,%22category%22:18%7D,%22language%22:%22en%22%7D&token=APP6_UEAAAAAWgLTub2MMho5DhfuQXUolcJiv9Egg6J2' 
    // );
    // curl.setOpt( 'FOLLOWLOCATION', true );
    // curl.setOpt( 'SSL_VERIFYPEER', false );
    
    // curl.on( 'end', function( statusCode, body, headers ) {
    //     resultObj.testCurl = statusCode + '---' + body.length + '---' + this.getInfo( 'TOTAL_TIME' );     
    //     trendData = JSON.parse(body.substr(6));

    //     resultObj.trendData = trendData;
    //     session.endDialog(JSON.stringify(resultObj));
    //     this.close();
    // });
     
    // curl.on( 'error', function(err, curlErrCode){
    //     console.error( 'Err: ', err );
    //     console.error( 'Code: ', curlErrCode );
    //     resultObj.testCurl = err + '---' + curlErrCode;     
    //     session.endDialog(JSON.stringify(resultObj));
    //     this.close();
    // } );
    // curl.perform();
    
    // ============== end test node-libcurl =================================================