import {Injectable} from '@angular/core';
import {AngularFireAuth} from 'angularfire2/auth';
import * as firebase from 'firebase';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class AuthService {
  // TODO: create custom type and map firebase object to custom type
  user$: Observable<firebase.User>;

  constructor(private afAuth: AngularFireAuth) {
    this.user$ = afAuth.authState;
  }

  login(email: string, password: string) {
    this.afAuth.auth.signInWithEmailAndPassword(email, password)
      .then(x => console.log('Logged in.', x))
      .catch(x => console.log('Unable to login.', x));
  }

  logout() {
    this.afAuth.auth.signOut()
      .then(x => console.log('Logged out.', x))
      .catch(x => console.log('Unable to log out.', x));
  }
}