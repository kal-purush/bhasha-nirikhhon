import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import { AngularFireAuth } from 'angularfire2/auth';
import * as firebase from 'firebase/app';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  
  private user: Observable<firebase.User>
  public userDetails: firebase.User = null;

  constructor(private _firebaseAuth: AngularFireAuth, private router: Router) {
    this.user = _firebaseAuth.authState;
    this.user.subscribe(
      (user) => {
        if (user) {
          this.userDetails = user;
          console.log('Inside Subscribe Code')
          console.log(this.userDetails);
        }
        else {
          console.log('Inside of subscribe code else');
          this.userDetails = null;
        }
      }
    );
  }

  signUpWithEmailAndPassword(email: string, password: string){
    if(this.userDetails == null){
      this._firebaseAuth.auth.createUserWithEmailAndPassword(email, password).then(x=>
        console.log(x));
    }
    else{
      return null;
    }
  }

  signInWithEmailAndPassword(email: string, password: string){
    if(this.userDetails == null){
      this._firebaseAuth.auth.signInWithEmailAndPassword(email, password).then(x=>
        console.log(x));
    }
    else{
      return null;
    }
  }

  signInWithGoogle() {
    //console.log('Signing in with google');
    return this._firebaseAuth.auth.signInWithPopup(
      new firebase.auth.GoogleAuthProvider()
    ).then(x=>console.log(x));
  }

  isLoggedIn() {
    if (this.userDetails == null ) {
        return false;
      } else {
        return true;
      }
    }
  logout() {
    console.log('Logged Out');
      this._firebaseAuth.auth.signOut()
      .then((res) => 
      this.router.navigate(['/']));
    }

  }