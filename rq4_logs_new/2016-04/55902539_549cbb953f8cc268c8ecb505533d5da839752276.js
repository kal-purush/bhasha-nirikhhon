var connection = Syncano({ apiKey: '7b72b1f8a0b00ad6bb8bf6f8c6dbf0107ef155c6' });
var Channel = connection.Channel;
var poll = Channel.please().poll({ instanceName: 'pulldata', name: 'telehealthchannel' });

var response;

poll.on('start', function() {
    console.log('poll::start');
});

poll.on('create', function(res) {
    console.log(res);
    $('#devID').text("Device ID: " + res.payload.DeviceID);
    $('#temp').text("Temperature: " + res.payload.Temperature);
    $('#pulse').text("Pulse: " + res.payload.Pulse);
    $('#breath').text("Breath: " + res.payload.Breath);
    $('#time').text("Timestamp: " + res.payload.created_at);
});

poll.start();