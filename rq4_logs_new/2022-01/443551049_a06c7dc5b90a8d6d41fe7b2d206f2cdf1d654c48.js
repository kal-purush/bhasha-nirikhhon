const express = require("express");
const https = require("https");

const app = express();

app.get("/",function(req, res){

   const url = "https://api.openweathermap.org/data/2.5/weather?q=Paris&units=metric&appid=df19c81cbc958ebd601330bdccafe2e5"

   https.get(url, function(response){
       console.log(response)
response.on("data", function(data){
    const weatherData = JSON.parse(data)
    const temp = weatherData.main.feels_like
    const weatherDescription = weatherData.weather[0].description
    console.log(weatherDescription)
})

   })



    res.send("Server is up and running")
})

app.listen(3000, function(){
    console.log("server is up and running on port 3000")
})