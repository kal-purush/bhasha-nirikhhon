var app = angular.module('metaCoinApp', []);

app.config(function ($locationProvider) {
  $locationProvider.html5Mode(true);
});

app.controller("metaCoinController", [ '$scope', '$location', '$http', '$q', '$window', '$timeout', function($scope , $location, $http, $q, $window, $timeout) {

  $scope.accounts = [];
  $scope.account = "";
  $scope.balance = "";

  $scope.refreshBalance = function() {
    var meta = MetaCoin.deployed();

    meta.getBalance.call($scope.account, {from: $scope.account}).then(function(value) {
      $timeout(function(){
        $scope.balance = value.valueOf();
      });
    }).catch(function(e) {
      console.log(e);
      setStatus("Error getting balance; see log.");
    });
  };

  $window.onload = function(){
    web3.eth.getAccounts(function(err, accs) {
      if (err != null) {
        alert("There was an error fetching your accounts.");
        return;
      }

      if (accs.length == 0) {
        alert("Couldn't get any accounts! Make sure your Ethereum client is configured correctly.");
        return;
      }

      $scope.accounts = accs;
      $scope.account = $scope.accounts[0];
      $scope.refreshBalance();
    });
  }

  $scope.sendCoin = function(amount, receiver) {
    var meta = MetaCoin.deployed();
    setStatus("Initiating transaction... (please wait)");

    meta.sendCoin($scope.receiver, $scope.amount, {from: $scope.account}).then(function() {
      setStatus("Transaction complete!");
      $scope.refreshBalance();
    }).catch(function(e) {
      console.log(e);
      setStatus("Error sending coin; see log.");
    });
  };

}]);