/**
 * Created by jose on 10/10/16.
 */
import {Http, Response} from '@angular/http'
import {Inject} from '@angular/core'
import {Observable} from 'rxjs/Observable'

export class CurrentLocationService {
    constructor(@Inject(Http) private http:Http) {}

    getCurrentLocation():Observable<Response> {
        return this.http.post('https://www.googleapis.com/geolocation/v1/geolocate?key=AIzaSyAjuKhx_vuFiovS_xuNN0ARd0Fj8koFwMk', [])
    }
}
