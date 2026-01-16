


/*

// Set global variables
var weatherData = {};

// After loading the page, get lat lon from ipinfo and execute the first function
$(document).ready(function() {
  $.get("http://ipinfo.io/geo", function(response) {
    var latLon = (response.loc).split(',');
    var latitude = latLon[0];
    var longitude = latLon[1];
    document.getElementById("city").innerHTML = "The current weather in<br>" + response.city;
    getWeatherDetail(latitude, longitude);
  }, "jsonp")
});

// Pull the weather from openweathermap and post the weather on the page
function getWeatherDetail(lat, lon) {
  var output = $.ajax({
    url: "http://api.openweathermap.org/data/2.5/weather?lat=" + lat + "&lon=" + lon + "&units=imperial&APPID=09449d6c9c3b937d6a67e55e34b1bfab",
    type: 'GET',
    data: {},
    dataType: 'json',
    success: function(data) {
    // assign the weatherData for future use, and set the page values to current weather
      weatherData = data;
      changeBackgroundImg(weatherData.weather[0].main);
      getFahrenheit();
      document.getElementById("conditions").innerHTML = weatherData.weather[0].description;
    },
    error: function(err) { alert(err); },
  });
};

// change the background image
function changeBackgroundImg(group) {
  document.body.style.backgroundImage = "url(https://s3.amazonaws.com/assetsanewdevio/fccweather/" + group + ".jpg)";
}

// get celsius temp from fahrenheit
function getCelsius() {
  document.getElementById("temp").innerHTML = Math.round((weatherData.main.temp - 32) * 5 / 9) + '° <a onclick="getFahrenheit()" href="#">C</span>';
}

// get fahrenheit from api data
function getFahrenheit() {
  document.getElementById("temp").innerHTML = Math.round(weatherData.main.temp) + '° <a onclick="getCelsius()" href="#">F</span>';
}
*/