import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';

/*
  Generated class for the DatafetchProvider provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular DI.
*/
@Injectable()
export class DatafetchProvider {
  data:any;
  constructor(public http: Http) {
    console.log('Hello DatafetchProvider Provider');
    this.load();
  }
  load() {
    if (this.data) {
      console.log("g");
      //return Promise.resolve(this.data);
      return new Promise(resolve => {


        this.http.get("https://vaioti.herokuapp.com/find")
          .map(res => res.json())
          .subscribe(data => {
              this.data = data;
              resolve(this.data);
              //console.log(data);
              console.log("reloaded");
            },
            err => {
              console.log("Gups!");
            }
          );
      });

    }
    return new Promise(resolve => {


      this.http.get("https://vaioti.herokuapp.com/find")
        .map(res => res.json())
        .subscribe(data => {
            this.data = data;
            resolve(this.data);
            //console.log(data);
            console.log("reloaded");
          },
          err => {
            console.log("Gups!");


          });

    });


  }

}