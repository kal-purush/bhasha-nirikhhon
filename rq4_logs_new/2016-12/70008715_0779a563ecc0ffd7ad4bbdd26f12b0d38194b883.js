bookRouter.controller('EditCtrl', ['$scope', '$http', '$stateParams', '$state', function($scope, $http, $stateParams, $state){

  $scope.listing="";
  if($stateParams.listingId !== "")
  {
    $scope.listId = $stateParams.listingId;
    $scope.route = "/api/listings/get/" + $scope.listId;

    $http.get($scope.route).success(function(response)
      {
        if(response)
        {
          $scope.listing = response;
          console.log("got dat data, filling in the edit form.");
        }
        else
        {
          alert("Your current route is invalid. The id you gave had no results. I'm going to route you back home.");
          $state.transitionTo('home');
        }
      })

    $scope.editListing = function()
    {

      $http.post('/api/listings/update', {
        listing_id: $scope.listId,
        title: $scope.listing.title,
        author: $scope.listing.author,
        isbn: $scope.listing.isbn,
        cost: $scope.listing.cost,
        stat: $scope.listing.stat

      }).success(function(data)
      {
          console.log("success, goin' home!");
          $state.transitionTo('home');
      }).error(function(data)
      {
          alert("You did something wrong.");
      })
    };
  }
  else
  {
    alert("Your current route is invalid. You need to include the listing id after the end slash. I'm going to route you back home.");
    $state.transitionTo('home');
  }
}]);