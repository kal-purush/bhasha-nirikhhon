var request = require('request');
var fs = require('fs');

var headers = {
    'X-API-TOKEN': 'cWXiYopljVUZKJq4uM9MaM8EUbUTW9jPEWKtFclV',
    'Content-Type': 'application/json'
};

var dataString = {
    "format": "csv"
};

var options = {
    url: 'https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_1IaPrI8eKeVqtbn/export-responses',
    method: 'POST',
    headers: headers,
    body: JSON.stringify(dataString)
};

function callback(error, response, body) {
    if (!error && response.statusCode == 200) {
        var data = JSON.parse(body);
        GetResponseExportProgress(data.result.progressId);
    }
}

request(options, callback);

function GetResponseExportProgress(progressId)
{
    var URI = 'https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_1IaPrI8eKeVqtbn/export-responses/'+ progressId
    var options1 = {
        url: URI,
        headers: headers
    };
    function callback(error, response, body) {
        if (!error && response.statusCode == 200) {
            var data = JSON.parse(body);
            console.log(data.result.status);
            if(data.result.status == "failed")
            {
                console.log("failed downloaded");
            }
            else if(data.result.status == "complete")
            {
                console.log("Complete downloaded ");
                GetResponseExportFile(data.result.fileId); 
            }
            else{
                console.log("Inside In Progress");
                request(options1, callback);
            }
        }
    }
    
    request(options1, callback);
}

function GetResponseExportFile(fileId){

    var URI = 'https://yourdatacenterid.qualtrics.com/API/v3/surveys/SV_1IaPrI8eKeVqtbn/export-responses/'+ fileId+'/file';
    var options2 = {
        url: URI,
        headers: headers
    };

    function callback(error, response, body) {
        if (!error && response.statusCode == 200) {
            console.log(body);
        }
    }
    request(options2, callback).pipe(fs.createWriteStream("test.csv"));
}