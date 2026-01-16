var widgets = angular.module('widgets', []);

widgets.controller('RestaurantCtrl',
  ['$scope',
  function($scope){

    $scope.restaurants = [];
    $scope.sort = "foodType";

    $scope.createRestaurant = function(name, foodType, imageURL) {
      var newRestaurant = {
        name: name,
        foodType: foodType,
        imageURL: imageURL
      };

      $scope.restaurants.push(newRestaurant);
      $scope.name = null;
      $scope.foodType = null;
      $scope.imageURL = null;
    };

    $scope.remove = function(restaurant) {
      var i = $scope.restaurants.indexOf(restaurant);
      console.log(i)
      $scope.restaurants.splice(i, 1);
    };

    $scope.setSort = function(selected) {
      $scope.sort = selected;
    };

  }]);