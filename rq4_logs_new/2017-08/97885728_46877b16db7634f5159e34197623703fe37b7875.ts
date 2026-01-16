import {Injectable, NgZone} from "@angular/core";
import firebase = require("nativescript-plugin-firebase");
import {Observable} from 'rxjs/Observable';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/share';

@Injectable()
export class FirebaseService {
  constructor(
    private ngZone: NgZone
  ){}

  private _allItems: Array<any> = [];
    
    
  resetPassword(email) {
    return firebase.resetPassword({
    email: email
    }).then((result: any) => {
          alert(JSON.stringify(result));
        },
        function (errorMessage:any) {
          alert(errorMessage);
        }
    ).catch(this.handleErrors);
  }

  handleSnapshot(data: any) {
    //empty array, then refill and filter
    this._allItems = [];
    if (data) {
      for (let id in data) {        
        let result = (<any>Object).assign({id: id}, data[id]);    
      }
    }
    return this._allItems;
  }

  getMyWishList(): Observable<any> {
    return new Observable((observer: any) => {
      let path = 'users';
      
        let onValueEvent = (snapshot: any) => {
          this.ngZone.run(() => {
            let results = this.handleSnapshot(snapshot.value);
            console.log(JSON.stringify(results))
             observer.next(results);
          });
        };
        firebase.addValueEventListener(onValueEvent, `/${path}`);
    }).share();              
  }

 

  getMyMessage(): Observable<any>{
    return new Observable((observer:any) => {
      firebase.getRemoteConfig({
      developerMode: false, // play with this boolean to get more frequent updates during development
      cacheExpirationSeconds: 300, // 10 minutes, default is 12 hours.. set to a lower value during dev
      properties: [{
      key: "message",
      default: "Happy Holidays!"
    }]
  }).then(
        function (result) {
          console.log("Fetched at " + result.lastFetch + (result.throttled ? " (throttled)" : ""));
          for (let entry in result.properties) 
            { 
              observer.next(result.properties[entry]);
            }
        }
    );
  }).share();
}

handleErrors(error) {
    console.log(JSON.stringify(error));
    return Promise.reject(error.message);
  }

}

    
