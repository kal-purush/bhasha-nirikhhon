
// code for posting responses to Google Sheet
window.addEventListener("load", function() {
    const form = document.getElementById('text-input');
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        const data = new FormData(form);
        const action = e.target.action;
        fetch(action, {
            method: 'POST',
            body: data,
        })
        createText();
    });
});

// code for retreiving and displaying responses
// Spreadsheet for testing
// https://docs.google.com/spreadsheets/d/1dMEF1bC9AuGRLN34wEfA6pBxsRev8cBJyUI-_5ZiDf4/pub?output=csv
// https://docs.google.com/spreadsheets/d/1dMEF1bC9AuGRLN34wEfA6pBxsRev8cBJyUI-_5ZiDf4

// used to create divs for new responses
let responses;
// used to stor individual texts and debug
let text;
// used to store all responses and prevent duplicates
let allResponses = [];

Papa.parse(
    "https://docs.google.com/spreadsheets/d/1dMEF1bC9AuGRLN34wEfA6pBxsRev8cBJyUI-_5ZiDf4/pub?output=csv",
    {
        download: true,
        header: true,
        //worker: true,
        complete: (results) => {
            console.log(results);
            responses = results;
            allResponses.push(results);
            createText();
        }
    }
);

function createText(parse = responses) {
    //let row = 0, col = 0;
    console.log("Data: ", parse.data);

    for (i in parse.data) {
        /*console.log ("live? ", responses.data[i].Live)
        if (responses.data[i].Live === "TRUE") {
            let colDiv = document.createElement("div");
            colDiv.classList.add("col");
        }; */

        let text = parse.data[i];
        //console.log(text);

        let textDiv = document.createElement("div");
        textDiv.classList.add("responses");
        textDiv.innerText = text.Responses;

        let display = document.getElementById("responses");
        display.appendChild(textDiv);
    };
};