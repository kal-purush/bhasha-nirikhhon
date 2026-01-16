import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class HardcodedAuthenticationService {

  constructor() { }

  authenticate = (username: string, password: string) => {
    if (username === 'Christian1' && password === '123') {
      sessionStorage.setItem('authenticatedUser', username);
      return true;
    }
    return false;
  }

  isUserLoggedIn = () => {
    const user = sessionStorage.getItem('authenticatedUser');
    return !(user === null);
  }

  logout = () => {
    sessionStorage.removeItem('authenticatedUser');
  }
}