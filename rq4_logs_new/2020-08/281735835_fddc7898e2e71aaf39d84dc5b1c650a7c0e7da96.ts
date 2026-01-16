
import readline from 'readline';
import {google} from 'googleapis';
import {OAuth2Client} from 'google-auth-library';
import fs from 'fs';

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = 'token.json';

const getClient = async (path: string = 'credentials.json') : Promise<OAuth2Client> => {
  // Load client secrets from a local file.
  const credentials = fs.readFileSync(path);

  const client = await authorize(JSON.parse(credentials.toString()));

  return client;
};

/**
 * Create an OAuth2 client with the given credentials, and then execute the
 * given callback function.
 * @param {Object} credentials The authorization client credentials.
 * @param {function} callback The callback to call with the authorized client.
 */
const authorize = async (credentials: any) : Promise<OAuth2Client> => {
  // eslint-disable-next-line camelcase
  const {client_secret, client_id, redirect_uris} = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(
      client_id, client_secret, redirect_uris[0]);

  // Check if we have previously stored a token.
  let token: string;
  if (fs.existsSync(TOKEN_PATH)) {
    token = fs.readFileSync(TOKEN_PATH, 'utf-8');
  } else {
    token = await getNewToken(oAuth2Client);
  }

  oAuth2Client.setCredentials(JSON.parse(token.toString()));

  return oAuth2Client;
};

/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 * @param {google.auth.OAuth2} oAuth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback for the authorized client.
 */
const getNewToken = async (oAuth2Client: OAuth2Client): Promise<string> => {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });
  console.log('Authorize this app by visiting this url:', authUrl);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve, reject) => {
    rl.question('Enter the code from that page here: ', (code) => {
      rl.close();
      oAuth2Client.getToken(code, (err, token) => {
        if (err) return reject(new Error('Error while trying to retrieve access token' + err));
        if (!token) return reject(new Error('No token'));

        const tokenString = JSON.stringify(token);

        // Store the token to disk for later program executions
        fs.writeFile(TOKEN_PATH, tokenString, (err) => {
          if (err) return console.error(err);
          console.log('Token stored to', TOKEN_PATH);
        });

        resolve(tokenString);
      });
    });
  });
};

export {authorize, getClient};