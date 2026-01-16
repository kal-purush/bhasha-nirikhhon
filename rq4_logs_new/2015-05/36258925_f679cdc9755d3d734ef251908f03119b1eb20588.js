fiestaProvider = function() {

  // Callbacks are application specific and start empty
  callbackNames = ['success', 'error', 'finally'];
  var callbacks = {};
  self = this;
  this.apiBaseUrl = 'https://runfiesta.com/v1/'
  this.$get = ['$http', function($http) {

    var api, apiParams, apiURL;

    api = function(config, opts) {
      var promise;
      opts = angular.extend({
        useLoadingView: true,
        debug: window.location.hostname === 'localhost' || true
      }, opts);
      angular.extend(config, {
        data: apiParams(config.data),
        params: apiParams(config.params),
        url: apiURL(config.url)
      });

      promise = $http(config).success(function(data, status) {
        if (opts.debug) console.log('API: ', config.url, data, status);
      }).error(function(data, status) {
        console.error('API ERROR: ', config.method, ' ', config.url, '\nStatus: ', status, '\nData: ', data);
      });

      angular.forEach(callbackNames, function(cbn) {
        promise[cbn](callbacks[cbn])
      });

      return promise;
    };

    angular.forEach(callbackNames, function(m) {
      callbacks[m] = function() {};
      self[m] = function(f) {
        callbacks[m] = f;
      };
      api[m] = function(f) {
        callbacks[m] = f;
      };
    });

    angular.forEach(['get', 'post', 'put', 'delete'], function(method) {
      var config;
      api[method] = function(url, params, opts) {
        config = {
          method: method.toUpperCase(),
          url: url
        };
        if (method === 'get') {
          config.params = params;
        } else {
          config.data = params;
        }
        return api(config, opts);
      };
    });

    api.user = function() {
      return JSON.parse(localStorage.user || null);
    };

    api.setUser = function(u) {
      return localStorage.user = JSON.stringify(u || null);
    };

    apiURL = function(path) {
      while (path.indexOf('/') === 0) {
        path = path.substr(1);
      }
      return this.apiBaseUrl + path;
    };

    apiParams = function(params) {
      params = angular.extend({
        authentication_token: localStorage.authentication_token
      }, params);
      return params;
    };

    return api

  }];

};

angular.module('fiesta', []).provider('fiesta', fiestaProvider);