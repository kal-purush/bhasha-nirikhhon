import { Injectable } from '@angular/core';

import { Router } from '@angular/router';
import { AngularFireAuth } from 'angularfire2/auth';
import * as firebase from 'firebase/app';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class AuthService {

  private user: Observable<firebase.User>;
  private userDetails: firebase.User = null;

  constructor(
    private firebaseAuth: AngularFireAuth,
    private router: Router) {
      this.user = firebaseAuth.authState;
    }

    isLoggedIn() {
      if (this.userDetails == null ) {
          return false;
      } else {
        return true;
      }
    }

    login(email: string, password: string) {
      this.firebaseAuth
        .auth
        .signInWithEmailAndPassword(email, password)
        .then(value => {
          console.log('Nice, it worked!');
          this.router.navigate(['/alarms']);
        })
        .catch(err => {
          console.log('Something went wrong:',err.message);
        });
    }

    logout() {
      this.firebaseAuth.auth.signOut()
        .then((res) => this.router.navigate(['/login']));
    }

}