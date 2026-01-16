const mongoose = require('mongoose');

const balanceSchema = new mongoose.Schema({
  crypto: String,
  address: String,
  amount: Number,
  usdValue: Number,
  date: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Balance', balanceSchema);