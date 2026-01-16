const mongoose = require('mongoose');

module.exports = function(mongoUrl) {
  mongoose.connect(mongoUrl);

  const Name = mongoose.model('Name', {
    name: String
  });

  return {
    Name

  };
}