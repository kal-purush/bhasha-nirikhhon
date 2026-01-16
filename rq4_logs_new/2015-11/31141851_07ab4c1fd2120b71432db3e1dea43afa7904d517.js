var $ = require('jquery');
var constants = require('appConstants');
var globals = require('globals');
var request = require('globals').request;
var promise2 = require('bluebird').promise;
var Promise = require('bluebird');
var _ = require('lodash');

var RuleRepository = (function () {

    var baseUrl = constants.ApiBaseUrl;

    var _rules = [];
    var _rulesPromise;

    var getRules = function () {

        if (_rules.length) {
          return Promise.resolve(_rules);
        }
        else if (_rulesPromise) {
          return _rulesPromise;
        }

        var result = request.get(baseUrl + 'rules');

        _rulesPromise = result.promise().then(function(result) {
            _rules = result.body;
            _rulesPromise = undefined;
            return result.body;
        });

        return _rulesPromise;
    };

    var getRuleById = function(id) {

      // Assuming the entire list is already loaded...
      return getRules().then(function(rules) {
        return _.find(rules, { id: id });
      });
    };

    var saveRule = function (rule) {

        return request.post(baseUrl + 'rules')
                      .send(rule)
                      .promise()
                      .then(function(result) {
                        return result.body;
                      });
    };

    var deleteRule = function (rule) {
      return request.del(baseUrl + 'rules/' + rule.id)
                    .send()
                    .promise();
    };

    return {
        getRules: getRules,
        getRuleById: getRuleById,
        saveRule: saveRule,
        deleteRule: deleteRule
    };

})();

module.exports = RuleRepository;