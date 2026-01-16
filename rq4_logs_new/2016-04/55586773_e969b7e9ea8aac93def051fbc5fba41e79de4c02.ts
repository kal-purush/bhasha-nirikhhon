declare function initItinerary(origin, destination): void;
declare function addMarker(latitude, longitude, title): void;
declare function getGPSCoordinates(cbSuccess, cbError): void;

export class Facade {

    static initItinerary(origin, destination) {
        initItinerary(origin, destination);
    }

    static addMarker(latitude, longitude, title) {
        addMarker(latitude, longitude, title);
    }

    static getGPSCoordinates(cbSuccess, cbError) {
        getGPSCoordinates(cbSuccess, cbError);
    }
}