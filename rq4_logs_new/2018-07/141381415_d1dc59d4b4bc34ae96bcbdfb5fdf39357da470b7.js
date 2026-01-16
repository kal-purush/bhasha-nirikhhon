const R = require('ramda');

module.exports = function(app) {
  app.post('/', (req, res) => {
    let filters = [];
    if (Array.isArray(req.query.filter)) {
      filters = req.query.filter;
    } else {
      filters.push(req.query.filter);
    }
    let filtered = R.pick( filters, req.body );
    res.send(filtered);
  });
};