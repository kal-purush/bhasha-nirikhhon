import {Injectable} from '@angular/core';
import {Router} from '@angular/router';
 
export class User {
  constructor(
    public email: string,
    public password: string) { }
}
 
var users = [
  new User('admin@admin.com','adm9'),
  new User('user1@gmail.com','a23')
];
 
@Injectable()
export class AuthenticationService {
 
  constructor(
    private _router: Router){}
 
  logout() {
    localStorage.removeItem("user");
    this._router.navigate(['login']);
  }

  authenticatedUser: User;

  login(user: User){
    this.authenticatedUser = users.find(u => u.email === user.email);
    if (this.authenticatedUser && this.authenticatedUser.password === user.password){
      localStorage.setItem("user", this.authenticatedUser.email);
      this._router.navigate(['private']);
      return true;
    }
    return false;
 
  }
 
   checkCredentials(){
    if (localStorage.getItem("user") === null){
        this._router.navigate(['login']);
    }
  }
}