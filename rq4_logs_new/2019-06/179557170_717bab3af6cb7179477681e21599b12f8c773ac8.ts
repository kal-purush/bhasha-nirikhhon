import {Injectable} from '@angular/core';
import {AngularFireDatabase, AngularFireList, AngularFireObject} from '@angular/fire/database';
import {AngularFireAuth} from '@angular/fire/auth';
import {User} from '../models/user';
import {FormGroup} from '@angular/forms';
import UserCredential = firebase.auth.UserCredential;

import {Router} from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class UserService {

  private userList: AngularFireList<any>;
  private user: AngularFireObject<any>;

  constructor(private DUSocialDB: AngularFireDatabase,
              private authService: AngularFireAuth,
              private router: Router) {
    this.userList = this.DUSocialDB.list('user-list');
  }

  addUser(registrationForm: FormGroup) {
    const newUser: User = {
      firstName: registrationForm.get('firstName').value,
      lastName: registrationForm.get('lastName').value,
      gender: registrationForm.get('gender').value,
      userName: registrationForm.get('userName').value,
      password: registrationForm.get('password').value,
      birthdate: registrationForm.get('birthdate').value,
      age: registrationForm.get('age').value,
      email: registrationForm.get('email').value.toString().trim()
    };

    // 1) add the user to Authentication Firebase.
    this.authService.auth.createUserWithEmailAndPassword(newUser.email, newUser.password)
      .catch((onFailure) => {
        console.log('Failed registering new user for Firebase authentication. Reason: ' + onFailure);
        alert(onFailure);
      })
      // 2) add it to the database.
      .then((onSuccess: UserCredential) => {
        console.log('Success registering new user for Firebase authentication. User:'
          + onSuccess.additionalUserInfo.username);
        alert('Check your email for email address verification.');
        this.authService.authState.subscribe((currentUser) => {
          currentUser.sendEmailVerification();
        });
        // push it to the user list.
        this.userList.push(newUser);
        this.router.navigateByUrl('login');
      });
  }

  get usersList() {
    return this.userList;
  }

  updateUser(user: User) {
    return this.user.update(user);
  }
}