/// <reference path="../../declarations/node.d.ts" />
import fs = require('fs')
var google = require('googleapis');

var OAuth2Client = google.auth.OAuth2;

var drive = google.drive('v2');

var googleAppSecrets = JSON.parse(fs.readFileSync('../../declarations/GoogleAppSecrets.json', 'utf8'));

var CLIENT_ID = googleAppSecrets.ClientId,
    CLIENT_SECRET = googleAppSecrets.ClientSecret,
    REDIRECT_URL = googleAppSecrets.RedirectUrl,
    SCOPE = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.readonly'];

var oauthClient = new OAuth2Client(CLIENT_ID, CLIENT_SECRET, REDIRECT_URL);

class Credentials{
    access_token: string;
    expiry_date: number;
    refresh_token: string;

    constructor(access_token:string, expiry_date:number, refresh_token?: string){
        this.access_token = access_token;
        this.expiry_date = expiry_date;
        if(refresh_token){
            this.refresh_token = refresh_token;
        }
    }

}


function getAccessToken(callback) {
    var filePath = '../../declarations/tokens.json';
    var credentials:Credentials;
    if (fs.existsSync(filePath)) {
        var tokenObject = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        if (false && tokenObject.access_token) {
            if (tokenObject.expiry_date && tokenObject.expiry_date > Date.now()) {
                credentials = new Credentials(tokenObject.access_token, tokenObject.expiry_date);
                oauthClient.credentials = credentials;
                callback();
                return;
            }
        }if (tokenObject.refresh_token) {
            console.log("Refreshing token");

            oauthClient.credentials.refresh_token = tokenObject.refresh_token;
            oauthClient.refreshAccessToken(function (error, tokens) {
                if (error) {
                    callback(error)
                } else {
                    credentials = new Credentials(tokens.access_token, tokens.expiry_date);
                    OAuth2Client.credentials = credentials;
                    callback();
                    return;
                }
            })
        }
    }
}


function getFile(getFileObject){
    getAccessToken(function(){
        console.log("Getting file ");
        drive.files.get(getFileObject, printResponse);
    })
}

var getFileObject = {
    fileId: '0B4R0_oRoHKYjUkRXVXlLbmdxcTQ',
    acknowledgeAbuse: true,
    auth: oauthClient
};

function printResponse(err, response){
    if(err){
        console.log("ERROR:", err);
    }else{
        console.log('inserted:', response);
    }
}

getFile(getFileObject);