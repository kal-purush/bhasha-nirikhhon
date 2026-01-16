/**
*   Trask test 1
*   Made by Anton Kretov,
*   anton@kretov.cz, 2018
*
*   Simple server-side part of the application. By using Express we get GET and POST requests with form data and process it
*   (save to file, load from file)
*
*   You can make use of this template to save your forms to your DB
*
 */


var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');
const path = require("path");

const FORM_VALIDATION_URL = '/process_form';                    // The URL of backend form handler
const SAVED_FORMS_URL = '/saved_forms';                         // The URL of backend form handler
const SERVER_PORT = '10101';
const LAST_ID_FILENAME = 'ids.txt';

app.use(bodyParser.urlencoded({
    extended: true
}));

app.use(bodyParser.json());
app.use(express.static('public'));

// Send the index page to the client
app.get('/', function(req, res) {
   res.sendFile(path.join(__dirname, '/' + 'public/index.html'));
});

// Get POST request with form's data and save it to the local file system
app.post(FORM_VALIDATION_URL, function(req, res) {
    if (fs.existsSync(path.join(__dirname, 'forms'))) {
        if(!fs.existsSync(path.join(__dirname, 'forms/' + LAST_ID_FILENAME))) {
            fs.writeFile(path.join(__dirname, 'forms/' + LAST_ID_FILENAME), '0', 'utf-8', function(error) {
                writeToFile(req, res);
            });
        } else {
            writeToFile(req, res);
        }
    } else {
        fs.mkdir(path.join(__dirname,'forms'), function(error) {
            if(error) console.error('Error ' + error);

            fs.writeFile(path.join(__dirname,'forms/' + LAST_ID_FILENAME), '0', 'utf-8', function(error) {
                writeToFile(req, res);
            });

        });
    }
});


function writeToFile(req, res) {
    fs.readFile(path.join(__dirname,'forms/' + LAST_ID_FILENAME), function(err, data) {
        if(isNaN(data)) throw 'Index file is corrupted! There is not a number there!';

        let id = parseInt(data);
        id++;
        let string = JSON.stringify(req.body, null, 4);

        fs.writeFile(path.join(__dirname,'forms/' +  LAST_ID_FILENAME), id, 'utf-8', function(error) {});

        fs.writeFile(path.join(__dirname,'forms/form_' + id + '.json'), string, 'utf-8', function(error) {
            if(error) throw error;

            // If everything is ok, send the feedback to the client
            res.header('content-type', 'text/plain');
            res.status(200);
            res.send('OK');
        });

    });
}

// Process GET request to show saved forms
app.get(SAVED_FORMS_URL, function(req, res) {
    
    let arrayOfSavedFormsInJSON = [];
    let files = fs.readdirSync(path.join(__dirname, 'forms'));

    // Sorting files to arrange it it the chronological orger
    files.sort(function(a, b) {
        return fs.statSync(path.join(__dirname, 'forms/') + a).mtime.getTime() -
            fs.statSync(path.join(__dirname, 'forms/') + b).mtime.getTime();
    });

    // Process all the files and put each form to the array
    files.forEach(function(file, index) {
       if(file === LAST_ID_FILENAME) return;
       let jsonObject = require(path.join(__dirname, 'forms/' + file));
       arrayOfSavedFormsInJSON.push(jsonObject);
    });

    // After we finish, send this data to the client
    res.writeHead(200, {"Content-Type": "application/json"});
    res.end(JSON.stringify(arrayOfSavedFormsInJSON));

});


// Create server
var server = app.listen(parseInt(SERVER_PORT), function() {
  var host = server.address().address;

  var port = server.address().port;

  console.log("App is listening at http://%s:%s", host, port);
});