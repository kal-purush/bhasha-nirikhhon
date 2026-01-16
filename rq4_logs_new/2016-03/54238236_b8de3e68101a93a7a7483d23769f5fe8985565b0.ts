import {Injectable} from 'angular2/core';
import {Http, Response} from 'angular2/http';
import {Observable} from 'rxjs/Rx';
import 'rxjs/add/operator/map'

@Injectable()
export class GuildRosterService {
  constructor (private http: Http) {}

  private _grUrl = 'https://nobreaksguild.com/v1/guild/members';  // URL to web api

  getRoster() {


    // auto switch url for dev environment
    if (window.location.hostname == 'localhost') {
      this._grUrl = 'http://localhost:5000/v1/guild/members';
    }

    return this.http.get(this._grUrl)
             .map((res:Response) => res.json())
  }

}