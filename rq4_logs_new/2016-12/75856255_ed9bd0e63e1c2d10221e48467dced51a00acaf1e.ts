import { Injectable } from '@angular/core';
import { Observer } from 'rxjs/Observer';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class Geolocator {

    public getLocation = function getLocation(opts): Observable<any> {
        return Observable.create(observer => {
            if (window.navigator && window.navigator.geolocation) {
                window.navigator.geolocation.watchPosition((position) => {
                    observer.next(position);
                }, (error) => {
                    switch (error.code) {
                        case 1:
                            observer.error('errors.location.permissionDenied');
                            break;
                        case 2:
                            observer.error('errors.location.positionUnavailable');
                            break;
                        case 3:
                            observer.error('errors.location.timeout');
                            break;
                    }
                }, opts);
            } else {
                observer.error('errors.location.unsupportedBrowser');
            }
        });
    }
}