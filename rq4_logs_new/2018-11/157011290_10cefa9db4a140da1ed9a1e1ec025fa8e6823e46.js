var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/fec');

var db = mongoose.connection;

db.on('error', function() {
  console.log('mongoose connection error');
});

db.once('open', function() {
  console.log('mongoose connected successfully');
});

var fireSchema = mongoose.Schema({
  _id: Number,
  accuracy: Number,
  communication: Number,
  cleanliness: Number,
  location: Number,
  checkin: Number,
  value: Number,
  reviews: [
    {
      accuracy: Number,
      communication: Number,
      cleanliness: Number,
      location: Number,
      checkin: Number,
      value: Number,
      username: String,
      date: String,
      comment: String,
      userImage: String
    }
  ],
  totalReviews: {
      average: Number,
      total: Number
   },
  flag: String
});

var fireBnb = mongoose.model('Property', fireSchema);

var selectAll = function(callback) {
  fireBnb.find({}, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  }).sort({_id: 1});
};

var selectId = function(id, callback) {
  fireBnb.find({_id: id}, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
};

var insertOne = function(property, callback) {
  fireBnb.create(property, callback);
}

var searchComments = function(string, callback) {
  // db.getCollection('properties').find({"reviews.comment" : {$regex : '.*String.*'}})
  fireBnb.find({"reviews.comment" : {$regex : `.*${string}.*`}}, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
};

// var selectProperty = function(id, callback) {
//   fireBnb.find({_id: id}, function(err, data) {
//     if(err) {
//       callback(err, null);
//     } else {
//       callback(null, data);
//     }
//   });
// };

module.exports.selectAll = selectAll;
module.exports.selectId = selectId;
module.exports.insertOne = insertOne;
module.exports.searchComments = searchComments;
// module.exports.selectProperty = selectProperty;