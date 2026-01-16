const mongoose = require('mongoose');

var BlockedUser = new mongoose.Schema({
  _user: {
    type: mongoose.Schema.Types.ObjectId,
    required: true,
    ref: 'User'
  },
  createdAt: {
    type: Date,
    expires: 30,
    default: Date.now
  }
});

var BlockedUser = mongoose.model('BlockedUser', BlockedUserSchema);

module.exports = {
  BlockedUser
};