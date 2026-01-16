var moment = require('moment');

console.log(moment().format());

var now = moment();

console.log('Current time stamp', now.unix());

var timestamp = 1507072378;
var currentMoment = moment.unix(timestamp);

console.log('Current moment: ', currentMoment.format('MMMM Do, YYYY @ H:mm A'));