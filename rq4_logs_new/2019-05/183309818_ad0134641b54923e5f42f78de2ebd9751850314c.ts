import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';

import { User } from '../shared/models/user'

@Injectable({
  providedIn: 'root'
})
export class UserServiceService {

  user_data = new User();

  constructor() {
    this.user_data = {
      user_id: 12,
      name: "juan",
      last_name: "last name",
      email: "last@gmail.com",
      password: ""
    }
  }

  createUser(user): void {
    console.log("user created:")
    console.log(user)
  }

  getUserById(user_id): User {
    return this.user_data;
  }
  updateUser(user, userId): void {
    console.log("user updated")
    console.log(user)
  }
}