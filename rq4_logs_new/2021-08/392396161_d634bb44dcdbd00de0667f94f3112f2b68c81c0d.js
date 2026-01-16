'use strict';

module.exports = function(City) {
  City.validatesLengthOf("name", {
    min: 3,
    message: {min : "City Name is too short"}
  });
};