app.factory('users', ['$http', function($http) {
  var userService = {
    users: [],

    getAll: function() {
      return $http.get('/allusers').then(function(data) {
  
        angular.copy(data.data, userService.users);
      });
    },
  }

  return userService;
}]);