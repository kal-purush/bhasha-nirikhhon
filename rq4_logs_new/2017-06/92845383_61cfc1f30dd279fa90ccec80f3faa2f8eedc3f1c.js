/**
 * PagerDuty Events to Twilio function
 * https://github.com/flyandi/hackpackv3-pagerduty
 */

/**
 * This function handles the update to multiple SIM cards
 */
var callbackTrigger = function(completed, max, callback, msg) {

 	if(completed >= max) {
      callback(null, msg)
  }
}

exports.handler = function(context, event, callback) {

  var _ = require('lodash');

  // parse pagerduty
  if(_.isArray(event.messages)) {

    let incidentType = event.messages[0].type;
  	let client = context.getTwilioClient();
    let sims = [/** List of SIM SID's */];
    let completed = 0;

    sims.forEach(function(sim, index) {

      client.preview.wireless.commands.create({
          command: incidentType,
          sim: sim
      }).then(function(response) {

        	completed++;

        	callbackTrigger(completed, sims.length, callback, 'triggered ' + incidentType);
      });

    });

    return;

  }

  return callback('Error: No incident type found', false);

};