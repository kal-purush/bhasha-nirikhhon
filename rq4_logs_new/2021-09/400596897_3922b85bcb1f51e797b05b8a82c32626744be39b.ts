import { Injectable } from '@angular/core';
import {MessageComponent} from "./message/message.component";
import {MatDialog} from "@angular/material/dialog";
import {BehaviorSubject} from "rxjs";

export interface User {
  avatar?: string
  login: string
  email: string
}

@Injectable({
  providedIn: 'root'
})
export class UserListService {

  constructor(public message: MatDialog) { }

  private users: User[] = [
    {avatar: './assets/github-default-icon.png', login: 'user1_nickname', email: 'user1@github.com'}
  ];

  public usersSubject$ = new BehaviorSubject<User[]>(this.users);

  updateUsers(user: User) {


    for (let i = 0; i < this.users.length; i++) {
      if (user.login === this.users[i].login) {
        console.log('Пользователь уже находится в каталоге');
        this.openMessage(`Пользователь ${user.login} уже находится в каталоге`);
        return;
      }
    }

    this.users.push(user);
    this.usersSubject$.next(this.users);
    this.openMessage(`Пользователь ${user.login} добавлен`);
  }


  openMessage(warning: string) {
    this.message.open(MessageComponent, {data: {warning}});
  }

  removeUser(user: User) {
    const index = this.users.findIndex(item => item.login === user.login);
    this.users.splice(index,1);
    this.usersSubject$.next(this.users);
  }
}