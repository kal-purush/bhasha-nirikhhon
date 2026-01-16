import { Injectable } from '@angular/core';
import { Angular2TokenService } from "angular2-token";
import { Response } from "@angular/http";
import { Observable } from "rxjs/Rx";
import { environment } from "../../environments/environment";
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class ObjectDataService {

  constructor(private _tokenService: Angular2TokenService) { 
    this._tokenService.init(environment.token_auth_config);
  }
  

  getSocialKinds(object: string): Observable<any> {
    return this._tokenService.get(object)
    .map((response: Response) => {
        return <any>response.json();
      })
    .catch(this.getSocialKinds);
  }
}