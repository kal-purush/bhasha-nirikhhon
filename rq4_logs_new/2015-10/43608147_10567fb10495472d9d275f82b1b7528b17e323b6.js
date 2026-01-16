var app = angular.module('MyApp');

app.factory('myFactory',['$http', function ($http) {
  return {
      get_user: function() {
          return $http({
              url: 'get-current-user',
              method: 'GET'
          })
      }
   }
}]);