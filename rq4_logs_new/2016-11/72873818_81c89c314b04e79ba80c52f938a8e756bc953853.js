var moment = require('moment');

console.log(moment().format());

var now = moment();

console.log('Current timestamp', now.unix());

var timestamp = 1478320913
var currentMoment = moment.unix(timestamp);

console.log('currrent moment', currentMoment.format('MMMM Do, YY @ h:mm A'));