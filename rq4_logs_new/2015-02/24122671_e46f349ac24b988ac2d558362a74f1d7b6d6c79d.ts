interface ILocationService {
    getLocation():{latitude:number; longitude:number};
}

angular.module('TruckMuncherApp').factory('LocationService', ['',
    () => new LocationService()]);

class LocationService implements ILocationService {

    constructor () {
    }

    getLocation() {
        navigator.geolocation.getCurrentPosition(function (pos) {

            return {latitude:pos.latitude, longitude:pos.longitude};

        }, function (error) {

            console.log(error);

        });
    }
}