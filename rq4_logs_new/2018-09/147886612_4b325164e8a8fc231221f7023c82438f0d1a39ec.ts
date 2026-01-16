import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor() { }

  registerUser(firstName, lastName, email, phone, password) {
    fetch(environment.host + '/api/users/register',
    {
      method: 'POST', // *GET, POST, PUT, DELETE, etc.
        mode: 'cors', // no-cors, cors, *same-origin
        cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
        credentials: 'same-origin', // include, same-origin, *omit
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            // 'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirect: 'follow', // manual, *follow, error
        referrer: 'no-referrer', // no-referrer, *client
        body: JSON.stringify({
          firstName,
          lastName,
          email,
          phone,
          password
        }),
    })
    .then(function(response) {
      return response.json();
    })
    .then(function(tokenJson) {
      localStorage.setItem('jwt-token', tokenJson.token);
      return {success: true };
    });
  }
  login(email, password) {
    fetch(environment.host + '/api/users/login',
    {
      method: 'POST', // *GET, POST, PUT, DELETE, etc.
        mode: 'cors', // no-cors, cors, *same-origin
        cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
        credentials: 'same-origin', // include, same-origin, *omit
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            // 'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirect: 'follow', // manual, *follow, error
        referrer: 'no-referrer', // no-referrer, *client
        body: JSON.stringify({
          email,
          password
        }),
    })
    .then(function(response) {
      return response.json();
    })
    .then(function(tokenJson) {
      localStorage.setItem('jwt-token', tokenJson.token);
      return {success: true };
    });
  }
  logout() {
    localStorage.removeItem('jwt-token');
  }
}