import { BehaviorSubject } from 'rxjs';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class AuthServiceService {
  loggedIn = new BehaviorSubject<boolean>(false);
  loggedName = new BehaviorSubject<string | null>(null);
  loggedprofilePic = new BehaviorSubject<string | null>(null);
  loggedInRoleState = new BehaviorSubject<string | null>(null);
  constructor() {}

  updateLoggedInState(state: boolean): void {
    this.loggedIn.next(state);
  }

  updateLoggedInRole(role: string | null): void {
    this.loggedInRoleState.next(role);
  }

  updateLoggedInName(value: string | null): void {
    this.loggedName.next(value);
  }

  updateLoggedprofilePic(value: string | null): void {
    this.loggedprofilePic.next(value);
  }

  isLoggedIn() {
    return this.loggedIn.asObservable();
  }

  loggedInRole() {
    return this.loggedInRoleState.asObservable();
  }

  loggedInName() {
    return this.loggedName.asObservable();
  }

  loggedInprofilePic() {
    return this.loggedprofilePic.asObservable();
  }

  onLogOut() {
    this.updateLoggedInState(false);
    this.updateLoggedInRole(null);
    this.updateLoggedInName(null);
    this.updateLoggedprofilePic(null);
  }
}