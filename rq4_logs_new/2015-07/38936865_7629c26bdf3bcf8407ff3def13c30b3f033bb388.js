(function bathroomsFactoryIIFE(){

  var bathroomsFactory = function($http) {
    var factory = {};
    factory.bathroom = {};
    factory.bathrooms = [];

    factory.getBathrooms = function() {
      return $http.get('http://localhost:3000/bathrooms').success(function(response){
        angular.copy(response, factory.bathrooms);
      })
    };

    factory.getBathroom = function(bathroomId) {
      return $http.get('http://localhost:3000/bathrooms/' + bathroomId).success(function(response){
        angular.copy(response, factory.bathroom)
      })
    };
    return factory;
  }

  bathroomsFactory.$inject = ['$http'];

  angular.module('mapApp').factory('bathroomsFactory')
})();