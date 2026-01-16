// ---

// import express and axios.
import express from "express";
import axios from "axios";
import bodyParser from "body-parser";

// create an express app and set the port number.
const app = express();
const port = 3000;

// openweather api utilities
const weatherApiKey = "";
const weatherUnit = "metric";
const weatherLanguage = "it";

// use the middleware.
app.use(bodyParser.urlencoded({ extended: true }));

// use the public folder for static files.
app.use(express.static("public"));

app.get("/", (req, res) => {
  res.render("index.ejs");
});

app.post("/", async (req, res) => {
  // nome città input
  const inputCity = req.body.id;

  // geolocalization city
  const coordinatesAPI = await axios.get(`http://api.openweathermap.org/geo/1.0/direct?q=${inputCity}&appid=${weatherApiKey}`);
  const coordinates = coordinatesAPI.data;

  // get latitude e longitude
  const latitude = coordinates[0].lat;
  const longitude = coordinates[0].lon;

  // get weather conditions
  const weatherAPI = await axios.get(`https://api.openweathermap.org/data/2.5/weather?lat=${latitude}&lon=${longitude}&units=${weatherUnit}&lang=${weatherLanguage}&appid=${weatherApiKey}`);
  const weather = weatherAPI.data;

  // objects
  const city = weather.name;
  const sysObj = weather.sys;
  const weatherObj = weather.weather[0];
  const mainObj = weather.main;
  const windObj = weather.wind;
  const cloudsObj = weather.clouds.all;

  res.render("index.ejs", {
    name: city,
    country: sysObj,
    weather: weatherObj,
    main: mainObj,
    wind: windObj,
    clouds: cloudsObj,
  });

});

// listen on your predefined port and start the server.
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});