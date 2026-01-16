const mongoose = require('mongoose');

const searchHistorySchema = new mongoose.Schema({
  userId: String,
  keyword: String,
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model('SearchHistory', searchHistorySchema);