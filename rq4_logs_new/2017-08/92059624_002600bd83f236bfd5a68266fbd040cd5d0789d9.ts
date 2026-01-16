import { Http } from '@angular/http';
import { environment } from '../../environments/environment';
import { Injectable } from '@angular/core';

@Injectable()
export class ComponentsService {
  private headers = new Headers({'Content-Type': 'application/X-www-form-urlencoded'});
  constructor(private http: Http) { }

  getComponents() {
    const url = environment.api_url + ':3000/components/';
    return this.http.get(url, {withCredentials: true})
    .toPromise()
    .then((res) => {
      return new Promise((resolve, reject) => {
        console.log(res);
        resolve(res);
      });
    });
  }

  getComponent(id: String): Promise<{}> {
    const url = environment.api_url + ':3000/components/' + id;
    return this.http.get(url, {withCredentials: true})
    .toPromise()
    .then((res) => {
      return new Promise((resolve, reject) => {
        console.log(res);
        resolve(res);
      });
    });
  }
}