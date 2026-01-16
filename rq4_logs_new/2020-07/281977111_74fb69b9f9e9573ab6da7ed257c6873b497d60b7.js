const db = require('./index');

let dbHelpers = {
  get: function(callback) {
    let queryStr = `SELECT * FROM food`;
    db.query(queryStr, (err, results) => {
      if (err) {
        callback(err);
      } else {
        callback(null, results);
      }
    });
  },
  post: function(req, callback) {
    let {name, rating, price, daveRATE} = req.body;
    let queryStr = `INSERT INTO food (name, rating, price, daveRATE) VALUES ("${name}", ${rating}, ${price}, ${daveRATE})`;
    db.query(queryStr, (err, results) => {
      if (err) {
        callback(err);
      } else {
        callback(null, results);
      }
    });
  }
}

module.exports = dbHelpers;

