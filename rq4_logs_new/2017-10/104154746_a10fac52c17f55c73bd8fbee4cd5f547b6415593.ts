import { Injectable } from '@angular/core';
import { User } from '../_model/user.model';

@Injectable()
export class AuthService {

  static isLoggedIn_aux = false;

  constructor() { }

  public isLoggedIn(): Boolean {
    return localStorage.getItem('user') != null;
  }

  login(user: User): void {
    // AuthService.isLoggedIn_aux = true;
    // Aplico timeout hasta resolver este problema:
    // ExpressionChangedAfterItHasBeenCheckedError: Expression has changed after it was
    // checked. Previous value: 'true'. Current value: 'false'.
    // Este error se da cuando estamos logueados y recargamos la pagina.
    // Con timeout en 0 damos cierta demora .
    setTimeout(() => AuthService.isLoggedIn_aux = true, 0);
    sessionStorage.setItem('user', JSON.stringify(user));
  }

  logout(): void {
    AuthService.isLoggedIn_aux = false;
    sessionStorage.setItem('user', null);
  }

}