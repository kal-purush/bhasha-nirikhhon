(function() {
  'use strict';

  angular
  .module('red')
  .factory('MarsApiFactory', MarsApiFactory);

  /** @ngInject */
  function MarsApiFactory($http) {

    var JOBS_GET_URL = 'https://red-wdp-api.herokuapp.com/api/mars/jobs';
    var COLONIST_POST_URL = 'https://red-wdp-api.herokuapp.com/api/mars/colonists';
    var ENCOUNTERS_GET_URL = 'https://red-wdp-api.herokuapp.com/api/mars/encounters';
    var ALIENS_GET_URL = 'https://red-wdp-api.herokuapp.com/api/mars/aliens';
    var ALIENS_POST_URL = 'https://red-wdp-api.herokuapp.com/api/mars/encounters';

    return {
      getEncounters: function(encounters) {
        return $http({
          method: 'GET',
          url: ENCOUNTERS_GET_URL
        });
      },
      getReport: function() {
        return $http({
          method: 'GET',
          url: ALIENS_GET_URL
        });
      },
      postReport: function(reportScope) {
        return $http({
          method: 'POST',
          url: ALIENS_POST_URL,
          data: {
            encounter: reportScope
          }
        });
      },
      getJobs: function() {
        return $http({
            method: 'GET',
            url: JOBS_GET_URL
          });
      },
      postColonist: function(colonistScope) {
        return $http({
          method: 'POST',
          url: COLONIST_POST_URL,
          data: {
            colonist : colonistScope
          }
        });
      }
    };
  }

})();