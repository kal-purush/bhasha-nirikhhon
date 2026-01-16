import { Injectable }     from '@angular/core';
import { Http, Response, Headers, RequestOptions } from '@angular/http';
import { Ride }           from '../models/ride';
import { Observable } from 'rxjs/Rx';


import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class RideService {
     // Resolve HTTP using the constructor
     constructor (private http: Http) {}
     // private instance variable to hold base url
     private ridesUrl = 'http://my.scoop.com:8080/notes'; 

     // Fetch all existing rides
     getRides() : Observable<Ride[]> {

         // ...using get request
         return this.http.get(this.ridesUrl)
                        // ...and calling .json() on the response to return data
                         .map((res:Response) => res.json())
                         //...errors if any
                         .catch((error:any) => Observable.throw(error.json().error || 'Server error'));

     }

     // Post a new Ride
    addRide (body: Object): Observable<Ride[]> {
        let bodyString = JSON.stringify(body); // Stringify payload
        let headers    = new Headers({
        	'Content-Type': 'application/json'
        	// ,'Authorization': `Bearer ${token}`
        }); // ... Set content type to JSON
        let options    = new RequestOptions({ headers: headers }); // Create a request option

        return this.http.post(this.ridesUrl, body, options) // ...using post request
                         .map((res:Response) => res.json()) // ...and calling .json() on the response to return data
                         .catch((error:any) => Observable.throw(error.json().error || 'Server error')); //...errors if any
    }   

    // Update a Ride
    updateRide (body: Object): Observable<Ride[]> {
        let bodyString = JSON.stringify(body); // Stringify payload
        let headers      = new Headers({ 'Content-Type': 'application/json' }); // ... Set content type to JSON
        let options       = new RequestOptions({ headers: headers }); // Create a request option

        return this.http.put(`${this.ridesUrl}/${body['id']}`, body, options) // ...using put request
                         .map((res:Response) => res.json()) // ...and calling .json() on the response to return data
                         .catch((error:any) => Observable.throw(error.json().error || 'Server error')); //...errors if any
    }   

    // Delete a Ride
    removeRide (id:string): Observable<Ride[]> {
        return this.http.delete(`${this.ridesUrl}/${id}`) // ...using put request
                         .map((res:Response) => res.json()) // ...and calling .json() on the response to return data
                         .catch((error:any) => Observable.throw(error.json().error || 'Server error')); //...errors if 

}
}