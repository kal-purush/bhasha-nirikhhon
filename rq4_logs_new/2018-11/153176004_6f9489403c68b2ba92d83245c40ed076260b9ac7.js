/* 
    CIS166AA - JavaScript
    Chapter 11
    Chapter case project

    Ajax with JSON-P

    Author: Andrew Dorre
    Date:   11/27/2018

    Filename: newsJSONP.js

    Powered by https://newsapi.org
*/
"use strict";

var url = 'https://newsapi.org/v2/top-headlines?' +
    'country=us&' +
    'apiKey=5cb35fea7dfd4098abd2498570bdcfb9';
var i, x, test = "";

var req = new Request(url);

function testFunc() {
    var object = fetch(req)
        .then(function (response) {
            console.log(response.json());
            //console.log(articles)                
            
        })
        /* .then(function (response) {
            
        var object = response.json();
        var myJSON = JSON.stringify(object);


        document.getElementById("test2").innerHTML = myJSON;
        }) */
    //var myJSON = JSON.stringify(object.articles[0]);
    //console.log(object);
    //var myJSON = JSON.parse(object);
    //var stringed = JSON.stringify(myJSON);
    for (i = 0; i < object.articles.length; i++) {
        test = object.articles[i];
        document.getElementById("test2").innerHTML = object.articles;
    }
    
    
    
}

function createEventListeners() {
    var buttonClick = document.getElementById("button");
    if (buttonClick.addEventListener) {
        buttonClick.addEventListener("click", testFunc, false);
    } else if (buttonClick.attachEvent) {
        buttonClick.attachEvent("onclick", testFunc);
    }
}

if (window.addEventListener) {
    window.addEventListener("load", createEventListeners, false);
} else if (window.attachEvent) {
    window.attachEvent("onload", createEventListeners);
}
/* for (i in req.articles) {
    x += "<h2>" + req.articles[i].title + "</h2>";            
} */
//document.getElementById("newstest").innerHTML = x
//document.getElementById("test2").innerHTML = req[0].response.body;


/* var url = 'https://newsapi.org/v2/top-headlines?' +
    'country=us&' +
    'apiKey=5cb35fea7dfd4098abd2498570bdcfb9';

var req = new Request(url);
fetch(req)
    .then(function (response) {
        console.log(response.json());
    })
document.getElementById("newstest").innerHTML = req.title;

if (window.addEventListener) {
    window.addEventListener("load", button, false);
} else if (window.attachEvent) {
    window.attachEvent("onload", button);
} */