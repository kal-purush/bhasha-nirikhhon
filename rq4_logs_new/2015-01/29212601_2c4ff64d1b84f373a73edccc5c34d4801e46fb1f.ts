/// <reference path="../../declarations/node.d.ts" />

import fs               = require('fs')
var google              = require('googleapis');
var readline            = require('readline');

var OAuth2Client        = google.auth.OAuth2;
var drive               = google.drive('v2');

export module GoogleAPICredentials {

    var googleAppSecrets = JSON.parse(fs.readFileSync('../../declarations/GoogleAppSecrets.json', 'utf8'));
    var tokenFilePath = '../../declarations/tokens.json';

    var CLIENT_ID = googleAppSecrets.ClientId,
        CLIENT_SECRET = googleAppSecrets.ClientSecret,
        REDIRECT_URL = googleAppSecrets.RedirectUrl,
        SCOPE = ['https://www.googleapis.com/auth/drive'];

    export var oauthClient = new OAuth2Client(CLIENT_ID, CLIENT_SECRET, REDIRECT_URL);


    class Credentials {
        access_token:string;
        expiry_date:number;
        refresh_token:string;

        constructor(access_token:string, expiry_date:number, refresh_token?:string) {
            this.access_token = access_token;
            this.expiry_date = expiry_date;
            if (refresh_token) {
                this.refresh_token = refresh_token;
            }
        }
    }

    export function initOAuthClient(callback) {
        var credentials:Credentials;
        if (fs.existsSync(tokenFilePath)) {
            var tokenObject = JSON.parse(fs.readFileSync(tokenFilePath, 'utf8'));
            if (tokenObject.access_token) {
                if (tokenObject.expiry_date && tokenObject.expiry_date > Date.now()) {
                    credentials = new Credentials(tokenObject.access_token, tokenObject.expiry_date);
                    oauthClient.credentials = credentials;
                    callback(oauthClient);
                    return;
                }
            }
            if (tokenObject.refresh_token) {
                console.log("Refreshing token");

                oauthClient.credentials.refresh_token = tokenObject.refresh_token;
                oauthClient.refreshAccessToken(function (error, tokens) {
                    if (error) {
                        callback(error)
                    } else {
                        credentials = new Credentials(tokens.access_token, tokens.expiry_date);
                        OAuth2Client.credentials = credentials;
                        saveCredentials(credentials);
                        callback(oauthClient);
                        return;
                    }
                });
            }
        }
        getAccessTokenRedirect(oauthClient, callback);
    }

    function getAccessTokenRedirect(oauthClient, callback) {
        var url = oauthClient.generateAuthUrl({
            access_type: 'offline', // will return a refresh token
            scope: SCOPE // can be a space-delimited string or an array of scopes
        });

        console.log('Visit the url: ', url);

        var rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        rl.question('Enter the code here:', function (code) {
            // request access token
            oauthClient.getToken(code, function (err, tokens) {
                if (err) {
                    console.log(err);
                    return;
                }
                console.log(JSON.stringify(tokens));
                // set tokens to the client
                // TODO: tokens should be set by OAuth2 client.
                oauthClient.setCredentials(tokens);
                var accessCredentials = new Credentials(tokens.access_token, tokens.expiry_date);
                saveCredentials(accessCredentials);
                callback(oauthClient);
            });
        });
    }

    function saveCredentials(credentials) {
        var credentialString:string = JSON.stringify(credentials);
        fs.writeFileSync(tokenFilePath, credentialString);
    }
}
