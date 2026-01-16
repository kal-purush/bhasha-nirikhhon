angular.module('botApp', [])
  .controller('botAppController', ['$scope', '$http', function ($scope, $http) {
    $scope.save = function () {
      var req = {
        method: 'POST',
        url: './keyword',
        headers: {
          'Content-Type': 'application/json'
        },
        data: {
          'key': $scope.key,
          'value': $scope.ans
        }
      }
      $http(req).then(function (res) {
        alert(res.data)
      })
    }
  }])