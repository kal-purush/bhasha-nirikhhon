const express = require("express");
const bodyParser = require("body-parser");
const https = require("https");

const app = express();
app.use(bodyParser.urlencoded({extended: true}))
// in order to use css & images on our server
app.use(express.static("public")) // we will be able to refer the files in this static directory with a relative url

// mailchimp api key: 2b484b68d9fa92b54b55e0455a2a7e50-us21a
// mailchimp audience_id: 523a0e3587

app.get("/", function(req, resp){
    console.log("On Sign-Up page...")
    resp.sendFile(__dirname + "/signup.html")
})

app.get("/success", function(req, resp){
    console.log("On Success page...");
    resp.sendFile(__dirname + "/success.html");
})

app.get("/failure", function(req, resp){
    console.log("On Failure page...");
    resp.sendFile(__dirname + "/failure.html");
})

app.post("/failure", function(req, resp){
    resp.redirect("/");
})

app.post("/", function(req, resp){
    const first_name = req.body.firstName;
    const last_name = req.body.lastName;
    const email = req.body.email;
    console.log(first_name);
    console.log(last_name);
    console.log(email);

    var data = {
        members: [{
            email_address: email,
            email_type: "text",
            status: "subscribed",
            merge_fields: {
                FNAME: first_name,
                LNAME: last_name
            }

        }]
        
    };

    var jsonData = JSON.stringify(data);

    const url = "https://us21.api.mailchimp.com/3.0/lists/523a0e3587";
    const options = {
        method:"POST",
        auth: "anytstring:2b484b68d9fa92b54b55e0455a2a7e50"
    }
    
    const request = https.request(url, options, function(response){

        response.on("data", function(data){
            console.log(JSON.parse(data));
        })

        if (response.statusCode === 200) {
            resp.sendFile(__dirname + "/success.html");
        }
        else {
            resp.sendFile(__dirname + "/failure.html");
        }
    })

    request.write(jsonData);
    request.end();

})

app.listen(3000, function(){
    console.log("Running server on port 3000...");
})