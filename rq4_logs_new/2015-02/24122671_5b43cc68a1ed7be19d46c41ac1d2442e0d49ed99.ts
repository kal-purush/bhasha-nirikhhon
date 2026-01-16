interface ITruckProfileView extends ng.IScope {

    allTrucks: Array<ITruckProfile>;
    selectedTruck: ITruckProfile;
    menu:Array<IMenu>;

    populateProfile(truck:TruckProfile, lat:number, lon, number);
}


angular.module('TruckMuncherApp').controller('truckProfileCtrl', ['$scope', 'growl', '$rootScope', 'TokenService', 'TruckService', 'TruckProfileService', 'SearchService', 'MenuService',
    function ($scope, growl, $rootScope, TokenService, TruckService, TruckProfileService, SearchService, MenuService) {

        var allTrucks = [];
        var lat;
        var lon;

        $scope.allTrucks = [];
        $scope.selectedTruck = {};
        $scope.menu = {};

        navigator.geolocation.getCurrentPosition(function (pos) {
            lat = pos.coords.latitude;
            lon = pos.coords.longitude;

            getTrucks();

        }, function (error) {
            growl.addErrorMessage('Unable to get location: ' + error.message);
        });

        $scope.populateProfile = function(truck, lat, lon) {
            var truckProfile = TruckProfileService.getTruckProfile(truck.id, lat, lon);
            var truckObj = {
                id: truck.id,
                icon: 'img/SingleTruckAnnotationIcon.png',
                coords: {
                    latitude: truck.latitude,
                    longitude: truck.longitude
                },
                truckProfile: new TruckProfile()
            };

            if (!_.isNull(truckProfile) && !_.isUndefined(truckProfile)) {
                truckObj.truckProfile = truckProfile;
            } else {
                truck.truckProfile = {name: "Could not find profile for truck"};
            }

            return truckObj;
        };

        $scope.onProfileClicked = function (truck) {
            $scope.selectedTruck = truck;
            console.log(truck);

        };

        $scope.$watch('selectedTruck', function () {
            if ($scope.selectedTruck && $scope.menu.truckId !== $scope.selectedTruck) {
                //setCustomMenuColors($scope.selectedTruck);
                console.log($scope.selectedTruck.id);
                MenuService.getMenu($scope.selectedTruck.id).then(function (response) {
                    $scope.menu = response;
                });
            }
        });

        function getTrucks() {
            $scope.loading = true;

            console.log("Got here");
            TruckService.getActiveTrucks(lat, lon).then(function (trucksResponse) {
                if (TruckProfileService.allTrucksInStoredProfiles(trucksResponse) && !TruckProfileService.cookieNeedsUpdate()) {
                    for (var i = 0; i < trucksResponse.length; i++) {
                        var truck = $scope.populateProfile(trucksResponse[i], lat, lon);
                        allTrucks.push(truck);
                    }
                } else {
                    TruckProfileService.updateTruckProfiles(lat, lon).then(function () {
                        for (var i = 0; i < trucksResponse.length; i++) {
                            var truck = $scope.populateProfile(trucksResponse[i],lat, lon);
                            allTrucks.push(truck);
                        }
                    });
                }
                //deferred.resolve(markers);
                $scope.loading = false;
                $scope.allTrucks = allTrucks;
            });

        }

        //$scope.searchTrucks = function (query) {
        //
        //    $scope.loading = true;
        //    SearchService.simpleSearch(query, 20, 0).then(function (results) {
        //
        //
        //        var allTrucks = _.map(results, function (r) {
        //            return r.truck.id;
        //        });
        //
        //
        //        $scope.loading = false;
        //    });
        //};


    }
]);
