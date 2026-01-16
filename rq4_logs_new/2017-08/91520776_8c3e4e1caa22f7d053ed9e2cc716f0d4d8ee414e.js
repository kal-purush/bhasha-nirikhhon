module.exports = function(app, cb) {
  var Event = app.models.event;
  var Participant = app.models.participant;
  Event.nestRemoting('participants');
  Participant.nestRemoting('payments');
  cb();
};