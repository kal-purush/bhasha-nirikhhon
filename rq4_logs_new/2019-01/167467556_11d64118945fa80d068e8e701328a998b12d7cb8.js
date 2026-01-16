// api.js
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
app.use(bodyParser.urlencoded({ extended: true  }));






const http = require('http');
const hostname = '127.0.0.1';
const port = 3000;
const fs = require('fs');


let urlDate = "";

const readline = require('readline');
const {google} = require('googleapis');

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/calendar.readonly'];
// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = 'token.json';

// Load client secrets from a local file.
fs.readFile('credentials.json', (err, content) => {
  if (err) return console.log('Error loading client secret file:', err);
  // Authorize a client with credentials, then call the Google Calendar API.
  authorize(JSON.parse(content), listEvents);
});

/**
 * Create an OAuth2 client with the given credentials, and then execute the
 * given callback function.
 * @param {Object} credentials The authorization client credentials.
 * @param {function} callback The callback to call with the authorized client.
 */
 function authorize(credentials, callback) {
  const {client_secret, client_id, redirect_uris} = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(
    client_id, client_secret, redirect_uris[0]);

  // Check if we have previously stored a token.
  fs.readFile(TOKEN_PATH, (err, token) => {
    if (err) return getAccessToken(oAuth2Client, callback);
    oAuth2Client.setCredentials(JSON.parse(token));
    callback(oAuth2Client);
  });
}

/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 * @param {google.auth.OAuth2} oAuth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback for the authorized client.
 */
 function getAccessToken(oAuth2Client, callback) {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });
  console.log('Authorize this app by visiting this url:', authUrl);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  rl.question('Enter the code from that page here: ', (code) => {
    rl.close();
    oAuth2Client.getToken(code, (err, token) => {
      if (err) return console.error('Error retrieving access token', err);
      oAuth2Client.setCredentials(token);
      // Store the token to disk for later program executions
      fs.writeFile(TOKEN_PATH, JSON.stringify(token), (err) => {
        if (err) console.error(err);
        console.log('Token stored to', TOKEN_PATH);
      });
      callback(oAuth2Client);
    });
  });
}

/**
 * Lists the next 10 events on the user's primary calendar.
 * @param {google.auth.OAuth2} auth An authorized OAuth2 client.
 */

 function listEvents(auth) {
  const calendar = google.calendar({version: 'v3', auth});
  calendar.events.list({
    calendarId: 'primary',
    
  }, (err, res) => {
    if (err) return console.log('The API returned an error: ' + err);
    const events = res.data.items;

    if (events.length) {
      //console.log('Upcoming 10 events:');


      for (let i = 0; i < events.length; i++) {
        var event = events[i];
        var start = event.start.dateTime ;
        var start = start.substr(0,10);


         console.log('%s - %s', start, event.summary);

       };
       console.log('--------------------------');
        console.log("veuillez choisissez une date: example '2013-02-14' ");
        console.log('--------------------------');


//a l'aide d un formulair 
app.get('/', (request, response) =>  response.sendFile(`${__dirname}/api.html`));

app.post('/', (request, response) => {
  //const postBody = request.body.data;
  var tmp = request.body.date;

  if (tmp === start) 
        { 
          response.send('le créneau que vous avez choisi est indisponible !. "' + request.body.date + '".');
          
          console.log("le créneau que vous avez choisi est indisponible !.") ;
        }else if (tmp !== start) {
          response.send('le créneau que vous avez choisi est disponible !. "' + request.body.date + '".');
          console.log("le créneau que vous avez choisi est disponible !.");

        }else{
          response.send("vide*-*");
          console.log("vide*-*");
        }
  
});


/*if we want use the console*/

        var stdin = process.openStdin();
        stdin.addListener("data", function(d) {       
        console.log("you entered: [" + d.toString().trim() + "]");
        if (d.toString().trim() === start) 
        { 
          console.log("le créneau que vous avez choisi est indisponible !.") ;
        }else if (d.toString().trim() !== start) {
          console.log("le créneau que vous avez choisi est disponible !.");

        }else{
          console.log("vide*-*");
        }

        });
            //var urlDate = '2019-01-20T21:00:00+01:00';
        


    } else {
      console.log('No upcoming events found.');
      
    }
  });
}







app.listen(3000, () => console.info('Application running on port 3000'));