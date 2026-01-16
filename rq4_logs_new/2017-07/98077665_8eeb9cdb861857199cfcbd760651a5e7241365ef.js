app.controller("adminloginctrl",function ($scope,$http) {
   $scope.adminlogin = function () {
       var url = "http://localhost:4000/adminlogin?adminUsername="+$scope.auname+"&adminPassword="+$scope.apwd;
         $http.get(url)
             .then(function(response){
                console.log(response)
             })
   }
});