var mqtt    = require('mqtt');

var client  = mqtt.connect('mqtt://localhost');
//var client = mqtt.connect('mqtt://test.mosquitto.org');

client.on('connect', function () {
    console.log('connected');
    client.subscribe('/iot/api/');
});

client.on('message', function (topic, data) {
    data = (!!data) ? JSON.parse(data) : data;

    console.log('topic ==>', topic.toString());
    console.log('payload ==>', data);
});


var interval = setInterval(function(client) {
    client.publish(
        '/iot/api/',
        JSON.stringify({param2: Math.random()}));
}, 3000, client);