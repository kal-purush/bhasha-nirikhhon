const https = require('https');

module.exports = {
    openSnippet: (userInput: string) => openSnippetFromUrl(userInput)
}

function openSnippetFromUrl(userInput: string) {
    let id = userInput.split("import/")[1];
    // let id = "V7NWC3Z5MHQ6FR4D";
    let url = generateUrl(id);
    getSnippet(url);
}

function generateUrl(id: string) {
    return "https://jayman-gameserver.herokuapp.com/conversations/" + id + "?startingIndex=0"
}

function getSnippet(url: string) {
    https.get(url, (resp: any) => {
        let data = '';

        // A chunk of data has been recieved.
        resp.on('data', (chunk: any) => {
            data += chunk;
        });

        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            console.log(JSON.parse(data));
        });

    }).on("error", (err: any) => {
        console.log("Error: " + err.message);
    });
}















