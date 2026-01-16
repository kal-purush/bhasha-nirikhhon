import * as reqp from 'request-promise-native';
import * as req from 'request';

export class TrapezeApiClient {
    private httpClient: req.RequestAPI<reqp.RequestPromise<any>, reqp.RequestPromiseOptions, req.RequiredUriUrl>;
    public constructor(public readonly endpoint: string) {
        this.httpClient = reqp.defaults({
            headers: {
                'User-Agent': 'Request-Promise'
            },
            json: true
        });
    }

    public getVehicleLocations(): reqp.RequestPromise<any> {
        const options = {
            uri: this.endpoint + '/internetservice/geoserviceDispatcher/services/vehicleinfo/vehicles',
            qs: {
                positionType: "CORRECTED",
                colorType: "ROUTE"
            },
            headers: {
                'User-Agent': 'Request-Promise'
            },
            json: true // Automatically parses the JSON string in the response
        };
        return this.httpClient
            .get(options);
    }
    public getRouteByTripId(vehicleId: string): reqp.RequestPromise<any> {
        const options = {
            method: 'POST',
            uri: this.endpoint + '/internetservice/geoserviceDispatcher/services/pathinfo/trip',
            qs: {
                id: vehicleId
            },
            headers: {
                'User-Agent': 'Request-Promise',
            },
            json: true
        };
        return this.httpClient
            .post(options);
    }
}