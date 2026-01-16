import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class AuthService {
  private userskey = 'users';
  private loggedInUserKey = 'loggedInUser';

  constructor() {}

  registerUser(userData: any): boolean {
    const users = JSON.parse(localStorage.getItem(this.userskey)|| '[]');
    const userExists = users.some((user: any) => user.Email === userData.Email);
    if (userExists) return false;

    users.push(userData);
    localStorage.setItem(this.userskey, JSON.stringify(users));
    return true;
  }

  
  login(email: string, password: string): boolean {
    const users =JSON.parse(localStorage.getItem(this.userskey) || '[]'); 
    const matchedUser = users.find((user:any)=>user.Email ===email && user.Password ===password); 

    if(matchedUser){
      localStorage.setItem(this.loggedInUserKey,JSON.stringify(matchedUser));
      return true
    }
    return false
  }
  // we are logging out current user
  logout(): void {
    // localStorage.getItem('user');
    localStorage.removeItem(this.loggedInUserKey);
  }
  getCurrentUser(): any {
    return JSON.parse(localStorage.getItem(this.loggedInUserKey) || 'null');
  }
}