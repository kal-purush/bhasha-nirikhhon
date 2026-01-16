(function () {
    angular.module("mcswcd").controller("AdminProfileCtrl", function ($scope, $routeParams, $http) {
        $scope.test = "Admin Profile";
        $http({
            url: '/api/allfarmer/',
            method: 'GET'
        }).success(function (data) {
            $scope.farmer = data;
            //module.exports
        }).error(function (data) {
            console.log(data);
            console.log('Error: ' + data);
        });
        $scope.isEditing = function () {
            $scope.edit = !$scope.edit;
        };
        $scope.isEditingModal = function () {
            $scope.editModal = !$scope.editModal;
        };

        $scope.listProduct = function(catagory) {
            return catagory.join(", ");
        };

        $scope.combineAddressInfo = function(address) {
            var fullAddress = "";
            for (loc in address){
                if (address[loc] != "" && loc != "lat" && loc != "lon"){
                    if (loc == 'state' || loc == 'city'){
                        fullAddress += ","
                    }
                    fullAddress += " " + address[loc];
                }
            }
            return fullAddress;
        };

        $scope.resetEditingModal = function() {
            $scope.editModal = false;
        }

        $scope.checkProductLength = function(prod) {
            return prod.length;
        }
        $scope.updateFarmer = function(userId, farmer) {
            console.log("userID", farmer);
            delete farmer.$$hashKey;
            console.log(farmer);
            $http({
                url: '/api/updateFarmer/' + userId  + '/' + JSON.stringify(farmer),
                method: 'POST'
            }).success(function(data) {
                console.log("success", data)
            }).error(function(data) {
                console.log("error", data)
            });
            $scope.isEditing();
        };
    });
})();