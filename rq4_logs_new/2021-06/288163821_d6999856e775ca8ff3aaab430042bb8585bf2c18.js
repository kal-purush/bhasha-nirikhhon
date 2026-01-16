const mongoose = require('mongoose');


const sourceSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    unique: true
  },
  description: {
    type: String,
    required: true
  },
  language: {
    type: String,
    required: true
  },
  image: {
    type: String,
    required: false
  },
  domain: {
    type: String,
    required: true
  },
  url: {
    type: String,
    required: true,
    unique: true
  },
  status: {
    type: String,
    required: false,
    default: "200"
  },
  SourceId: {
    type: String,
    required: false
  },
  tags: [
    {
      type: String
    }
  ],
  private: {
    type: Boolean,
    default: true
  },
  refreshAt: {
    type: Date,
    default: Date.now
  }
}, {
  timestamp: true
});

module.exports = mongoose.model('Source', sourceSchema);