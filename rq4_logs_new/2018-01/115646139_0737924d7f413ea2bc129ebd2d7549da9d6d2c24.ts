import { Injectable } from '@angular/core';
import * as firebase from 'firebase';
import {AuthService} from './auth.service';
import {AngularFireDatabase} from 'angularfire2/database';
import {Router} from '@angular/router';

@Injectable()
export class RenderMyPageService {
  usersList;
  listObservable;
  loggedUserFirstName;
  loggedUserLastName;
  constructor(private router: Router,
              protected authService: AuthService,
              private database: AngularFireDatabase) { }
  renderUserInfo (email: string, password: string) {
    firebase.auth()
      .signInWithEmailAndPassword(email, password)
      .then( metadata => {
        console.log('metadatadshrshrst', metadata);
        // some function that handles the stuff
        this.authService.successfulLog = true;
        this.usersList = this.database.list('users');
        this.listObservable = this.usersList.snapshotChanges();
        this.usersList.valueChanges().subscribe(users => {
          if (users) {
            console.log('email', metadata.email);
            console.log('users', users);
            this.getUserByEmail(metadata.email)
              .then( loggedUser => {
                console.log('dataFromGetByMail', loggedUser);
                localStorage.setItem('loggedUserFirstName', JSON.stringify(loggedUser.firstName));
                localStorage.setItem('loggedUserLastName', JSON.stringify(loggedUser.lastName));
                localStorage.setItem('loggedUserEmail', JSON.stringify(loggedUser.email));
                // this.authService.currUser = JSON.parse(localStorage.getItem('currUser'));
                // this.loggedUserFirstName = localStorage.getItem('loggedUserFirstName');
                // this.loggedUserLastName = localStorage.getItem('loggedUserLastName');
              });
          }
        });
        this.router.navigate(['me']); // goes in the end
      })
      .catch((error) => {
        const errorCode = error.code;
        const errorMessage = error.message;
        alert( errorMessage);
      });
  }
  getUserByEmail(email: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.database.list('users').valueChanges().subscribe(userList => {
        for ( let i = 0; i < userList.length; i++) {
          if (userList[i]['email'] === email) {
            resolve(userList[i]);
          } else {
            reject('no user');
          }
        }
      });
    });
  }
}