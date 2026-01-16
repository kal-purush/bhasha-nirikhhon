var NATS = require('nats');
var server1 = NATS.connect('nats://ruser:T0pS3cr3t@gnatsd:4222');
// var server2 = NATS.connect();

//
// request/reply example
//
console.log('NodeJS Server #1 Listening to channel "api"')
server1.subscribe('api', {'queue':'api.requests'}, function(request, replyTo) {
  console.log('Handling ' + request);
  server1.publish(replyTo, 'NodeJS Server #1: ' + request);
});
// server2.subscribe('help', function(request, replyTo) {
//   setTimeout(function() {
//     server1.publish(replyTo, 'NodeJS Server #2: ' + request);
//   }, 500);
// });