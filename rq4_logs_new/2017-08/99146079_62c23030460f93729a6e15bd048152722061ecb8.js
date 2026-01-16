var BikeTracker = require('./../js/bike-tracker.js').bikeModule;

var getCount = function(location, count) {
  $('#seattle-bikes').text(count);
};

$(document).ready(function() {
  var newBikeTracker = new BikeTracker();
  var location = "Seattle";
  newBikeTracker.getBikes(location, getCount);
});