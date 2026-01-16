(function() {
  app.factory('ApiService', ApiService);

  ApiService.$inject = ['$http'];

  function ApiService ($http) {
    var sendXHR = function (method, url,params) {
      return $http({
        method: method,
        url: url,
        params: params
      });
    }

    return {
    	getBanks: function () {
    		return sendXHR('GET', '/banks');
    	},

      getCities: function(params) {
        return sendXHR('GET', '/getCitiesByBank', params);
      },
    	
      getBranches: function (params) {
    		return sendXHR('GET', 'branches', params)
    	}
    }
  }
})();