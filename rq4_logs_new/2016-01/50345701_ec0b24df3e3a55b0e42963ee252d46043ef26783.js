var City = require('./cityInfo.js');

var fs = require('fs');
var express = require('express');
var app = express();

//============================================================================//

var PrintRequestDetails = function(req, res, next){
  var padding = (req.method.length == 3) ? '  -- ' : ' -- ';
  console.log(req.method + padding + Date() + ' -- ' + req.url);
  next();
};

//============================================================================//

var sendAllStations = function(req, res){
  res.send(cities.bangalore.getAllStations()); 
};

var sendAllBuses = function(req, res){
  var source = req.query.source;
  var destination = req.query.destination;
  var allBuses = {};
  allBuses.direct = cities.bangalore.getBus(source, destination);
  allBuses.routes = cities.bangalore.getAlternateBus(source, destination);
  res.send(allBuses);
};

//============================================================================//

app.use(PrintRequestDetails);
app.use(express.static('./public'));

app.get('/all_stations', sendAllStations);
app.get('/getAllBuses', sendAllBuses);
//============================================================================//
//Controller

var cities = {};

var getBusData = function(){
  var data = fs.readFileSync('./data/busRoutes.txt', 'utf-8').split('\r\n');
  data = data.map(function(busInfo){
    busInfo = busInfo.split(':');
    var bus = {};
    bus.busNo = busInfo[0];
    bus.stations = busInfo[1].split(',');
    return bus;
  });
  return data;
};

var main = function(){
  var bangalore = new City();
  var busData = getBusData();
  busData.forEach(function(bus){
    bangalore.addBus(bus.busNo);
    bus.stations.forEach(function(station){
      bangalore.addStation(station);
      bangalore.addStationToBus(bus.busNo, station);
    });
  });
  cities.bangalore = bangalore;
}; 

main();
//============================================================================//




module.exports = app;