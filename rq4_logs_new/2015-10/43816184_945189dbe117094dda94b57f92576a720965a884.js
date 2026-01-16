// JavaScript Document

  // Initializing the map.
  // by default, all mouse and touch interactions on the map are enabled.
  // and it has zoom and attribution controls
  var central = [1.345, 103.81] // Central Singapore 1.3667 103.8
  var loc1 = [1.352785, 103.702211] // Jurong West
  var loc2 = [1.331958, 103.942179] // Bedok North
  var loc3 = [1.310644, 103.847033] // KK Hosp
  
  //Get the data
//  d3.csv("data/data.csv", function(error, data) {
//    data.forEach(function(d) {
//    d.date = parseDate(d.date);
//   d.sales = +d.sales;
//    });

  // Set the default view of the map and display onto webpage
  var map = L.map('helloMap')
    .setView(central, 11);
  
  // Set the marker positions
  var marker1 = L
        .marker(loc1)
        .addTo(map);
  
  var marker2 = L
        .marker(loc2)
        .addTo(map);
  
  var marker3 = L
        .marker(loc3)
        .addTo(map);
  
  var markerPopup = L
        .marker(loc3)
        .addTo(map);
    
  // Add a tile layer
  // A mapbox account was created to get the accessToken
  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery � <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'weiann-lim.ciev0axre0v9grxm2t535wkkq',
    accessToken: 'pk.eyJ1Ijoid2VpYW5uLWxpbSIsImEiOiJjaWV2MGF5NGQwdmd2cjhtMDluZ2QwM2Y3In0.ipv9DdnDcQ4mh-PW_Duflg'
}).addTo(map);
  
  // Trying out popup
  marker.bindPopup("<b>Hello World!!!</b><br>I am a popup.").openPopup();
  
  