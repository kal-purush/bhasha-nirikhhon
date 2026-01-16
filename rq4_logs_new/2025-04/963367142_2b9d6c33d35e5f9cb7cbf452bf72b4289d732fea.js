const mongoose = require('mongoose');

const reportSchema = new mongoose.Schema({
    userId: String,
    handle: String,
    status: String,
    detectedAt: Date
});

module.exports = mongoose.model('Report', reportSchema);