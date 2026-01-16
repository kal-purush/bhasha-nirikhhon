import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Http, Response, RequestOptions, Headers, RequestMethod } from '@angular/http';

import { TSMap } from 'typescript-map';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/do';

import { User } from '../../_model/user.model';
import { RestClientService } from '../../_service/rest-client.service';
import { URL_USER_LIST, URL_BASE, URL_USER_ADD, URL_USER_DELETE } from '../../../environments/rest-environment';


@Injectable()
export class UsersService {

  constructor(private _restClientService: RestClientService) { }

  public list(): Observable<User[]> {
    const obs: Observable<Response> = this._restClientService.list(URL_BASE, URL_USER_LIST);
    obs.subscribe(rs => console.log(rs));
    return obs.map((res: Response) => <User[]>res.json())
      .do(data => console.log('data' + JSON.stringify(data)));
  }


  insert(user: User): Observable<TSMap<string, number>> {
    return this._restClientService.insert(
      user,
      URL_BASE, URL_USER_ADD
    ).map( (res: Response) => <TSMap<string, number>>new TSMap().fromJSON(res.json()));
  }


  delete(user: User): Observable<Response> {
    return this._restClientService.delete(
      user,
      URL_BASE, URL_USER_DELETE,
      // ).map( (res: Response) => res.text() );
    );
  }

}