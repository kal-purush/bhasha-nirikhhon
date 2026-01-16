import {Injectable} from "@angular/core";
import {Headers, Http, Response} from "@angular/http";

import 'rxjs/add/operator/toPromise';

import {User} from "../user";
import {UserService} from "./user.service";

@Injectable()
export class LoginService {
  private headers = new Headers({'Content-Type': 'application/json'});
  private usersUrl = 'app/users';

  loggedInUser: User;

  // store the URL so we can redirect after logging in
  redirectUrl: string;

  constructor(private http: Http, private genericService: UserService) {
  }

  private getLoginUser(username: string): Promise<User> {
    return this.genericService.getUsers()
        .then(users => users.find(user => user.username === username));
  }

  login(username: string, password: string): Promise<User> {
    return this.getLoginUser(username).then(user => {
      if (user && user.password === password) {
        this.loggedInUser = user;
        return user;
      } else {
        throw new Error(`Either the username ${username} is unknown or the password is invalid!`);
      }
    });
  }

  logout(): void {
    this.loggedInUser = null;
    this.redirectUrl = null;
  }

  loggedIn(): boolean {
    return this.loggedInUser != null;
  }

  private handleError(error: any): Promise<any> {
    console.error('An error occurred', error); // for demo purposes only
    return Promise.reject(error.message || error);
  }
}