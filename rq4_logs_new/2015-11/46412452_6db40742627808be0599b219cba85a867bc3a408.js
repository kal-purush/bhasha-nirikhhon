var mongoose = require('mongoose');

module.exports = function () {
  mongoose.connect(process.env.MONGOLAB_URI);

  var storeSchema = mongoose.Schema({
      fetched: Number,
      count: Number,
      stores: [{
        store: String,
        stock: Number
      }]
  });
  var Store = mongoose.model('Store', storeSchema);

  return {
    save: function (stores, success, error) {
      new Store(stores).save(function (err) {
        if (err && typeof(error) == 'function') error(err);
        else if (typeof(success) == 'function') success();
      });
    },
    getLatestFetch: function (success, error) {
      Store.findOne({}).sort({fetched: -1}).exec(function (err, stores) {
        if (!err && typeof(success) == 'function') success(stores);
        else if (typeof(error) == 'function') error(err);
      });
    }
  };
}();