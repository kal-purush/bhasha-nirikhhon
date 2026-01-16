import { Injectable } from '@angular/core';
import { AuthHttp } from 'angular2-jwt';
import { ENV } from '@app/env';
import 'rxjs/add/operator/map';

/*
  Generated class for the AdminServiceProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.
*/
@Injectable()
export class AdminServiceProvider {

  constructor(public authHttp: AuthHttp) {
    console.log('Hello AdminServiceProvider Provider');
  }

  public getSensors(): Promise<any> {
    const getSensors = ENV.api + "/getsensors";
    console.log(getSensors);
    
    return this.authHttp.get(getSensors).toPromise();
  }


}