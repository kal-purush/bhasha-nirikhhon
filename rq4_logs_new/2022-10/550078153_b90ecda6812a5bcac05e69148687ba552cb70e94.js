const express = require("express");
const app = express();
const https = require("https");
const bodyParser=require("body-parser");

app.use(bodyParser.urlencoded({extended:true}));
app.get("/", function(req, res) {
  res.sendFile(__dirname+"/index.html")

})
app.post("/",function(req,res){

  console.log(req.body.cityName);
  const query="London";
  const apiKey="ed57d903e89d55983d6451a6d63febf2";
  const units="metric";
  const url = "https://api.openweathermap.org/data/2.5/weather?q="+query+"&appid="+apiKey+"&units="+units;
  https.get(url, function(response) {
    console.log(response.statusCode);
    response.on("data", function(data) {

      const weatherData = JSON.parse(data);
      console.log(weatherData);

      const weatherDescription = weatherData.weather[0].description;
      console.log("aici");
      console.log(weatherDescription);

      const weatherImage="http://openweathermap.org/img/wn/"+weatherData.weather[0].icon+"@2x.png";

      res.write("<h1>Current Temperature in "+query+" is "+weatherData.main.temp+ " *Celsius</h1>");
      res.write("<p> The weather is currently "+weatherDescription+"</p>");
      res.write("<img src="+weatherImage +">");
      res.send();
    })

  });
})
//



app.listen(3000, function() {
  console.log("Server started at localhost:3000");
})