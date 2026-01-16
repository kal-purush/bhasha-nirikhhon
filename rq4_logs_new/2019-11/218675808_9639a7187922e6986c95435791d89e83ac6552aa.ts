import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable()
export class MapService {

    private geoCoder;

    public geocodeLocation(location: string): Observable<any> {
        this.geoCoder = new (<any>window).google.maps.Geocoder();

        return new Observable((observer) => {

            this.geoCoder.geocode({address:location}, (result,status) => {
                if (status === 'OK') {

                    const geometry = result[0].geometry.location;
                    observer.next({lat: geometry.lat(), lng:geometry.lng()});
                 } else {
                        observer.error('Location cannot be geocoded');
                    }
                    
                });
            });

        }
    }