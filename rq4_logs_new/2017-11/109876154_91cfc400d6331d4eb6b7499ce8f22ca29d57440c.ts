import {Injectable} from '@angular/core';
import {User} from '../model/user.model';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/of';

@Injectable()
export class UserService {

  users: User[] = [{
    id: 11,
    firstName: 'Hongsuk',
    lastName: 'Choi',
    email: 'choi@gmail.com',
    password: '1234',
  }, {
    id: 12,
    firstName: 'Davis',
    lastName: 'Louie',
    email: 'davislouie90@gmail.com',
    password: '4321'
  }, {
    id: 13,
    firstName: 'Zoe',
    lastName: 'Ramirez',
    email: 'zoe@gmail.com',
    password: '1234'
  }, {
    id: 14,
    firstName: 'Sudip',
    lastName: 'Baral',
    email: 'sudip@gmail.com',
    password: '1234'
  }];

  getAllUsers(): Observable<User[]> {
    // console.log(this.users);
    return Observable.of(this.users);
  }

  getUserById(id: number): Observable<User> {
    return Observable.of(this.users.find(user => user.id === id));
  }

  addUser(user: User): void {
    this.users.push(user);
  }

  editUserById(editUser: User): void {
    const updateUser = this.users.find(user => user.id === editUser.id);
    updateUser.id = editUser.id;
    updateUser.firstName = editUser.firstName;
    updateUser.lastName = editUser.lastName;
    updateUser.email = editUser.email;
    updateUser.password = editUser.password;
  }

  deleteUserById(id: number): void {
    this.users.find(user => user.id === id);
  }

}