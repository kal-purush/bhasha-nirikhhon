// var PythonShell = require("python-shell");
const mqtt = require("mqtt");
var sensorLib = require("node-dht-sensor");
const client = mqtt.connect("mqtt://test.mosquitto.org:1883");
//22 é o tipo do sensor e 12 é o pino GPIO
sensorLib.initialize(22, 12);

// PythonShell.run("smoke.py", function(err) {
//   if (err) throw err;
//   console.log("finished");
// });

// client.subscribe("smoke");

// client.on("connect", () => {
//   console.log("connected");
// });

// client.on("message", (topic, message) => {
//   console.log("received message %s %s", topic, message);
// });

var interval = setInterval(function() {
  read();
}, 2000);
function read() {
  var readout = sensorLib.read();
  client.publish("temperature", readout.temperature.toFixed(2));
  client.publish("humidity", readout.temperature.toFixed(2));
  console.log(
    "Temperature: " +
      readout.temperature.toFixed(2) +
      "C, " +
      "humidity: " +
      readout.humidity.toFixed(2) +
      "%"
  );
}