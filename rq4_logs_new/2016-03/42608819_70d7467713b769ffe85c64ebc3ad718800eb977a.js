var _ = require('lodash'),
    models = require('../services/models'),
    utils = require('../utils'),
    returnSuccess = utils.returnSuccess,
    validateAsync = require('../utils').validateAsync,
    router = require('express').Router();

module.exports = router;

var constraints = {
  code: {
    presence: { message: '^You must supply the account code.'}
  }
}

router.route('/')
  .get(function(req, res, next) {
    var filter = req.query;
    res.promise = models.Account.findAll(filter);
  })

  .post(function(req, res, next) {
    var obj = req.body;
    res.promise = validateAsync(obj, constraints).then(function() {
      return models.Account.create(obj)
    }).then(returnSuccess)
  })

router.route('/:code')
  .get(function(req, res, next) {
    var code = req.params.code;
    res.promise = models.Account.findOne({code: code})
  })

  // Unlike the typical scenario, you can PUT to a non-existing
  // account and it will be created instead of returning an error.
  .put(function(req, res, next) {
    var obj = req.body,
        code = req.params.code

    obj.code = code

    res.promise = validateAsync(obj, constraints).then(function() {
      return models.Account.findOne({code: code})
    }).then(function(row) {
      if ( row == null ) return models.Account.create(obj);
      else return models.Account.update(row._id, obj);
    }).then(returnSuccess)
  })

  .delete(function(req, res, next) {
    var code = req.params[0];
    res.promise = models.Account.findOne({code: code}).then(function(row) {
      utils.assertNotNull(row, 'No such Account ' + code);

      return models.Account.destroy({_id: row._id})
    }).then(returnSuccess)
  })