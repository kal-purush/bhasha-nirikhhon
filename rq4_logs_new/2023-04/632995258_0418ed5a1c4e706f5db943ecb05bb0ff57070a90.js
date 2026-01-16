//jshint esversion:6


const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const https = require('https');

const ejs = require("ejs");
const _ = require("lodash");
const { response } = require('express');

const app = express();

app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static('public'));

app.get('/', function (req, res) {
    res.render("home");
});

app.get('/about', function (req, res) {
    res.render("about");
});

app.get('/skills', function (req, res) {
    res.render("skills");
});

app.get('/projects', function (req, res) {
    res.render("projects");
});

app.get('/contact', function (req, res) {
    res.render("contact");
});

app.post('/contact', function (req, res) {

    const firstName = req.body.fname;
    const lastName = req.body.lname;
    const email = req.body.email;
    const phone = req.body.phone;
    const message = req.body.message;

    const data = {
        members: [{
            email_address: email,
            status: "subscribed",
            merge_fields: {
                FNAME: firstName,
                LNAME: lastName,
                PHONE: phone,
                MESSAGE: message
            }
        }
        ]
    };

    const jsonData = JSON.stringify(data);

    const url = "https://us8.api.mailchimp.com/3.0/lists/266d57da53";

    const options = {
        method: "POST",
        auth: "b1tsh3ll:9d94cfe8190787da55d3da9fd5a6e86a-us8"
    }

    const request = https.request(url, options, function(response) {
        if (response.statusCode === 200) {
            res.redirect("/");
        } else {
            res.render("failure", {statusResponse: response.statusCode});
        }
        response.on("data", function(data) {
            console.log(JSON.parse(data));
        })
    })

    request.write(jsonData);
    request.end();

})

app.post("/failure", function(req, res) {
    res.redirect("/contact");
})


app.listen(process.env.PORT || 3000, function (req, res) {
    console.log('Server is Running On Port 3000!');
});