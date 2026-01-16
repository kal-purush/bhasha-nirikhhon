angular.module("routerApp").controller('templateController', templateController);



function templateController($scope)
{
    $scope.date = new Date();
 
};