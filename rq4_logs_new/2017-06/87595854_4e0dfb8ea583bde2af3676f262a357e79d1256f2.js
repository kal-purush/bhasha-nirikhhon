var path = require('path');
var fs = require('fs');
var _ = require('underscore');

var Activities = {
  file_path: path.resolve(path.dirname(__dirname), 'data/activities.json'),

  get: function() {
    return JSON.parse(fs.readFileSync(this.file_path, 'utf-8')).activities;
  },

  setActivity: function(activities) {
    var lastId = this.getLastId() + 1;

    var data = {
      activities: activities,
      lastId: lastId,
    };

    fs.writeFileSync(this.file_path, JSON.stringify(data), 'utf8');
  },

  getLastId: function() {
    return JSON.parse(fs.readFileSync(this.file_path, 'utf-8')).lastId;
  },
};

module.exports = Activities;