interface ITruckProfileTruckScope extends ng.IScope {
    isOnline:boolean;
    selectedTruck:ITruckProfile;
    customMenuColors:CustomMenuColors;
    activeTruck:IActiveTruck;
    activeTrucks:Array<IActiveTruck>;
    truckCoords:{latitude:number; longitude:number};
    map:{center:{}; zoom:number};

}
angular.module('TruckMuncherApp').controller('truckProfileTruckCtrl', ['$scope', 'growl', '$stateParams', 'TruckProfileService', 'colorService', 'TruckService',
    ($scope, growl, $stateParams, TruckProfileService, ColorService, TruckService) => new TruckProfilePartialCtrl($scope, growl, $stateParams, TruckProfileService, ColorService, TruckService)]);

class TruckProfilePartialCtrl {
    constructor(private $scope:ITruckProfileTruckScope,
                private growl:IGrowlService,
                private $stateParams:ng.ui.IStateParams,
                private TruckProfileService:ITruckProfileService,
                private colorService:IColorService,
                private TruckService:ITruckService) {

        var lat, lon;
        $scope.isOnline = false;
        $scope.selectedTruck = null;

        navigator.geolocation.getCurrentPosition(function (pos) {
            lat = pos.coords.latitude;
            lon = pos.coords.longitude;

            TruckService.getActiveTrucks(lat, lon).then(function (results) {

                $scope.selectedTruck = TruckProfileService.getTruckProfile($stateParams.id);
                $scope.customMenuColors = colorService.getCustomMenuColorsForTruck($scope.selectedTruck);

                for (var i = 0; i < results.trucks.length; i++) {
                    $scope.activeTrucks.push(results.trucks[i]);
                }

                $scope.activeTruck = _.find($scope.activeTrucks, function (x) {
                    return x.id === $scope.selectedTruck.id;
                });

                if ($scope.activeTruck !== null) {
                    $scope.isOnline = true;
                }

                $scope.truckCoords = {latitude: $scope.activeTruck.latitude, longitude: $scope.activeTruck.longitude};
                $scope.map.center = {
                    latitude: $scope.activeTruck.latitude,
                    longitude: $scope.activeTruck.longitude
                }

            });
        }, function (error) {
            growl.addErrorMessage('Unable to get location: ' + error.message);
        });
    }
}