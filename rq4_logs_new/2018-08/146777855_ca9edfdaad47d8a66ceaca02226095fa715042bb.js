const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

var Client = require('node-rest-client').Client;

var client = new Client();

const app = express();

app.use(cors())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json());

app.post('/getLocations',(req,res)=> {
const typedword = req.body.city;
    // direct way
client.get("https://maps.googleapis.com/maps/api/place/textsearch/json?query="+typedword+"&key=AIzaSyDJb-BinRkanrRQJs86E04S_M8TVSed55k", function (data, response) {
    // parsed response body as js object

    res.json({msg:true, data:data});
    // raw response
   console.log(response);
});



})

app.listen(4010,()=>{
    console.log('listentening to port');
})