import { Inject, Injectable, Optional }                      from '@angular/core';
import { Http, Headers, URLSearchParams }                    from '@angular/http';
import { RequestMethod, RequestOptions, RequestOptionsArgs } from '@angular/http';
import { Response, ResponseContentType }                     from '@angular/http';
import 'rxjs/add/operator/map';
import { Observable } from 'rxjs/Observable';
import { Driver } from '../interfaces/Driver';
import { User } from '../interfaces/User';
import { Request } from '../interfaces/Request';
import { Rider } from '../interfaces/Rider';

@Injectable()
export class DatabaseApi {
  private url = 'http://192.241.235.189:8080';
  public defaultHeaders: Headers = new Headers();

  constructor (protected http: Http) {

  }

  //------------------------Driver Api---------------------------
  
  public GetDriver(id: string):Observable<Driver> {
    let path = this.url + '/driver/' + id;
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Get,
      headers: headers
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public GetDriverList():Observable<Driver[]> {
    let path = this.url + '/driver';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');
    return this.http.get(path)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PostDriver(driver: Driver):Observable<Driver> {
    let path = this.url + '/driver';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Post,
      headers: headers,
      body: driver
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  //------------------------Rider Api---------------------------

  public GetRider(id: string):Observable<Rider> {
    let path = this.url + '/rider/' + id;
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Get,
      headers: headers
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public GetRiderList():Observable<Rider[]> {
    let path = this.url + '/rider';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');
    return this.http.get(path)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PostRider(rider: Rider):Observable<Rider> {
    let path = this.url + '/rider';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Post,
      headers: headers,
      body: rider
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PutRider(rider: Rider):Observable<Rider> {
    let path = this.url + '/rider';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Put,
      headers: headers,
      body: rider
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

//------------------------Request Api---------------------------

  public GetRequest(id: string):Observable<Request> {
    let path = this.url + '/request/' + id;
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Get,
      headers: headers
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public GetRequestList():Observable<Request[]> {
    let path = this.url + '/request';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');
    return this.http.get(path)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PostRequest(request: Request):Observable<Request> {
    let path = this.url + '/request';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Post,
      headers: headers,
      body: request
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PutRequest(request: Request):Observable<Request> {
    let path = this.url + '/request';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Put,
      headers: headers,
      body: request
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

//------------------------User Api---------------------------

  public GetUserByUser(user:User):Observable<User> {
    let path = this.url + '/user/user/' + user.username;
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Get,
      headers: headers
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public GetUserById(id: string):Observable<User> {
    let path = this.url + '/user/id/' + id;
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Get,
      headers: headers
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public GetUserList():Observable<User[]> {
    let path = this.url + '/user';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');
    return this.http.get(path)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PostUser(user: User):Observable<User> {
    let path = this.url + '/user';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Post,
      headers: headers,
      body: user
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

  public PutUser(user: User):Observable<User> {
    let path = this.url + '/user';
    let headers = new Headers(this.defaultHeaders.toJSON());
    headers.set('Content-Type', 'application/json');

    let requestOptions:RequestOptionsArgs = new RequestOptions({
      method: RequestMethod.Put,
      headers: headers,
      body: user
    });
    return this.http.request(path, requestOptions)
      .map((data) => {
        if(data.status === 500) {
          return undefined
        } else {
          return data.json() || {};
        }
      });
  }

}