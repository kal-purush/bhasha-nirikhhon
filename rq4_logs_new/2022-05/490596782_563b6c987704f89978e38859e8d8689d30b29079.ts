import { Injectable } from '@angular/core';
import { AngularFireAuth } from "@angular/fire/compat/auth";
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
 export class AuthenticationService {
  // userData: Observable<firebase.User>;

  constructor(private auth: AngularFireAuth) {
    // this.userData = auth.authState;
  }

  /* Sign up */
  SignUp(email: string, password: string) {
    this.auth
      .createUserWithEmailAndPassword(email, password)
      .then((res: any) => {
        console.log('You are Successfully signed up!', res);
      })
      .catch((error: any) => {
        console.log('Something is wrong:', error.message);
      });
  }

  /* Sign in */
  SignIn(email: string, password: string) {
    this.auth
      .signInWithEmailAndPassword(email, password)
      .then((res: any) => {
        console.log('You\'re in!', res);
      })
      .catch((err: any) => {
        console.log('Something went wrong:', err.message);
      });
  }

  /* Sign out */
  SignOut() {
    this.auth
      .signOut()
      .then((res: any) => {
        console.log('You\'re out!');
      })
      .catch((err: any) => {
        console.log('Something went wrong:', err.message);
      });;
  }
}