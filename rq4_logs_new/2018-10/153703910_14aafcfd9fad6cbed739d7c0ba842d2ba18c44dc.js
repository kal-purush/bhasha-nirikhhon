// See https://github.com/dialogflow/dialogflow-fulfillment-nodejs
// for Dialogflow fulfillment library docs, samples, and to report issues
'use strict';
 
const functions = require('firebase-functions');
const {WebhookClient} = require('dialogflow-fulfillment');
const {Card, Suggestion} = require('dialogflow-fulfillment');
 const
    express = require('express'),
    body_parser = require('body-parser'),
    app = express().use(body_parser.json());
app.listen(process.env.PORT || 1337, () => console.log('webhook is listening'));

app.get('/', (req, res) => {
    res.send('Hello World!');
});
app.post('/webhook', (req, res) => {
    // FILL IN
});
process.env.DEBUG = 'dialogflow:debug'; // enables lib debugging statements
 
exports.dialogflowFirebaseFulfillment = functions.https.onRequest((request, response) => {
  const agent = new WebhookClient({ request, response });
  console.log('Dialogflow Request headers: ' + JSON.stringify(request.headers));
  console.log('Dialogflow Request body: ' + JSON.stringify(request.body));
 
  function welcome(agent) {
    agent.add(`Welcome to my agent!`);
  }
 
  function fallback(agent) {
    agent.add(`I didn't understand`);
    agent.add(`I'm sorry, can you try again?`);
}

  function keyMetricsTrack(agent) {
    let body = request.body;
    console.log(body.queryResult.parameters);
    switch(body.queryResult.parameters.KeyMetrics){
        case 'small team':
            agent.add('57% of our people are in teams of 6-12 in September. which is 2% improvement from August results');
            break;
        case 'showcase':
            agent.add('Showcase metric had significant improvement in September. Results moved from 37% to 63% over the last month');
            break;
        case 'test and deployment automation':
            agent.add('September result for Test and deployment automation metric were 54% which is 2% improvement from August results');
            break;
        case 'autonomous':
            agent.add('The result for the autonomy metric for September is 67%.');
            break;
    }
  }
  
  function keyMetricsCal(agent) {
    let body = request.body;
    console.log(body.queryResult.parameters);
    switch(body.queryResult.parameters.KeyMetrics){
        case 'small team':
            agent.add('57% of our people are in teams of 6-12 in September. which is 2% improvement from August results');
            break;
        case 'showcase':
            agent.add('Showcase metric had significant improvement in September. Results moved from 37% to 63% over the last month');
            break;
        case 'test and deployment automation':
            agent.add('September result for Test and deployment automation metric were 54% which is 2% improvement from August results');
            break;
        case 'autonomous':
            agent.add('The result for the autonomy metric for September is 67%.');
            break;
    }      
  }
  
  function deliveryMetricsTrack(agent) {
      
  }
  
  function deliveryMetricsCal(agent) {
      
  } 
  
  function operationsMetricsTrack(agent) {
      
  }

  // // Uncomment and edit to make your own intent handler
  // // uncomment `intentMap.set('your intent name here', yourFunctionHandler);`
  // // below to get this function to be run when a Dialogflow intent is matched
  // function yourFunctionHandler(agent) {
  //   agent.add(`This message is from Dialogflow's Cloud Functions for Firebase editor!`);
  //   agent.add(new Card({
  //       title: `Title: this is a card title`,
  //       imageUrl: 'https://developers.google.com/actions/images/badges/XPM_BADGING_GoogleAssistant_VER.png',
  //       text: `This is the body text of a card.  You can even use line\n  breaks and emoji! 💁`,
  //       buttonText: 'This is a button',
  //       buttonUrl: 'https://assistant.google.com/'
  //     })
  //   );
  //   agent.add(new Suggestion(`Quick Reply`));
  //   agent.add(new Suggestion(`Suggestion`));
  //   agent.setContext({ name: 'weather', lifespan: 2, parameters: { city: 'Rome' }});
  // }

  // // Uncomment and edit to make your own Google Assistant intent handler
  // // uncomment `intentMap.set('your intent name here', googleAssistantHandler);`
  // // below to get this function to be run when a Dialogflow intent is matched
  // function googleAssistantHandler(agent) {
  //   let conv = agent.conv(); // Get Actions on Google library conv instance
  //   conv.ask('Hello from the Actions on Google client library!') // Use Actions on Google library
  //   agent.add(conv); // Add Actions on Google library responses to your agent's response
  // }
  // // See https://github.com/dialogflow/dialogflow-fulfillment-nodejs/tree/master/samples/actions-on-google
  // // for a complete Dialogflow fulfillment library Actions on Google client library v2 integration sample

  // Run the proper function handler based on the matched Dialogflow intent name
  let intentMap = new Map();
  intentMap.set('Default Welcome Intent', welcome);
  intentMap.set('Default Fallback Intent', fallback);
  intentMap.set('SEO_delivery_metrics_followup_calculate', deliveryMetricsCal);
  intentMap.set('SEO_delivery_metrics_followup_track', deliveryMetricsTrack);
  intentMap.set('SEO_key_metrics_followup_calculate', keyMetricsCal);
  intentMap.set('SEO_key_metrics_followup_track', keyMetricsTrack);
  intentMap.set('SEO_operations_metrics_followup_track', operationsMetricsTrack);
  // intentMap.set('your intent name here', yourFunctionHandler);
  // intentMap.set('your intent name here', googleAssistantHandler);
  agent.handleRequest(intentMap);
});