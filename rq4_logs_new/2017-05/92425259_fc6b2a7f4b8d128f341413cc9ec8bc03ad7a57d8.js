var express = require("express");
var alexa = require("alexa-app");
var fs = require("fs");
var https = require("https");
var app = express();
var _ = require('lodash');

var PORT = process.env.port || 8081;

// ALWAYS setup the alexa app and attach it to express before anything else.
// This is the URI to post, eg: localhost:8081/test
var alexaApp = new alexa.app("test");

alexaApp.express({
  expressApp: app,

  // verifies requests come from amazon alexa. Must be enabled for production.
  // You can disable this if you're running a dev environment and want to POST
  // things to test behavior. enabled by default.
  checkCert: false,

  // sets up a GET route when set to true. This is handy for testing in
  // development, but not recommended for production. disabled by default
  debug: true
});

// now POST calls to /test in express will be handled by the app.request() function

// from here on you can setup any other express routes or middlewares as normal
app.set("view engine", "ejs");


alexaApp.launch(function(request, response) {
  response.say("You launched the app!");
});

// A sampel of building utterances
alexaApp.dictionary = { "names": ["matt", "joe", "bob", "bill", "mary", "jane", "dawn"],
                        "services": ["timer", "google"] };

alexaApp.intent("openSthIntent", {
  "slots": {"SERVICE": "LITERAL"},
  "utterances": ["start {a|} {services|SERVICE}", "launch {a|} {services|SERVICE}", 
                  "open {a|} {services|SERVICE}"]
  },
  function(request, response) {
    console.log("in openSthIntent");

    // get the service name by read the slot 'SERVICE'
    var serviceName = request.slot('SERVICE');
    var reprompt = "I didn't hear any file or service.";
    if(_.isEmpty(serviceName)) {
      console.log("in empty");
      var prompt = "Tell me the name of file or service";
      response.say(prompt).reprompt(reprompt).shouldEndSession(false);
      return true;
    } else {
      console.log("in non empty");
      var spawn = require('child_process').spawn;
      if (serviceName == 'google') {
        console.log("in goolgle");
        spawn('open', ['http://www.google.com']);
        response.say("Your service loaded").reprompt("Anything else I can help?").shouldEndSession(false);
      }
      else if (serviceName == "timer") {
        console.log("in timer");
        spawn('open', ['file:///Users/Victor/Google%20Drive/OCBC/IT%20Tools/Web/TCCTimer/TccTimer.aspx.html']);
        response.say("Your service loaded").reprompt("Anthing else I can help?").shouldEndSession(false);
      }
      else {
        console.log("in else");
        response.say("I'm sorry I can't handle your request").reprompt("Try another one").shouldEndSession(false);
      }
      return false;
    }
});

alexaApp.intent("AMAZON.StopIntent", function(request, response){
  console.log("Stop");
})

var intentStr = {
  "version": "1.0",
  "session": {
    "new": false,
    "sessionId": "amzn1.echo-api.session.abeee1a7-aee0-41e6-8192-e6faaed9f5ef",
    "attributes": {},
    "application": {
      "applicationId": "amzn1.echo-sdk-ams.app.000000-d0ed-0000-ad00-000000d00ebe"
    },
    "user": {
      "userId": "amzn1.account.AM3B227HF3FAM1B261HK7FFM3A2"
    }
  },
  "request": {
    "type": "IntentRequest",
    "requestId": "amzn1.echo-api.request.6919844a-733e-4e89-893a-fdcb77e2ef0d",
    "intent": {
      "name": "launchTimerIntent"
    }
  }
};

alexaApp.intent("nameIntent", {
    "slots": { "NAME": "LITERAL" },
    "utterances": [
      "my {name is|name's} {names|NAME}", "set my name to {names|NAME}"
    ]
  },
  function(request, response) {
    console.log("in nameIntent");
    response.say("Success!");
  }
);

/*alexaApp.request(intentStr)
  .then(function(response) {
    console.log(JSON.stringify(response, null, 3));
  });
*/
app.listen(PORT);
console.log("Listening on port " + PORT + ", try http://localhost:" + PORT + "/test");

// When Alexa calls the web service, port 443 for https must be used.
// If the program needs to bind port 443, need to run with root in mac.
var httpsPort = 443;
https.createServer({
  //key: fs.readFileSync(__dirname + "/self_cert/private-key.pem"),
  //cert: fs.readFileSync(__dirname + "/self_cert/certificate.pem")
  key: fs.readFileSync(__dirname + "/../cert/server.key"),
  cert: fs.readFileSync(__dirname + "/../cert/server.crt")
}, app).listen(httpsPort);

console.log("Listening on port " + httpsPort + ", try https://localhost:" + httpsPort + "/test");