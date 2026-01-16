import { Injectable } from '@angular/core';
import { auth } from 'firebase/app';
import { Router } from '@angular/router';
import { AngularFireAuth } from '@angular/fire/auth';
import { Credentials } from './user.model';

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  constructor(private router: Router, private afAuth: AngularFireAuth) { }

  googleSignIn = async () => {
    const provider = new auth.GoogleAuthProvider();
    const credentials = await this.afAuth.auth.signInWithPopup(provider);
    return this.updateUserData(<any>credentials.user);
  }

  emailAndPasswordSignIn = async (email: string, password: string) => {
    const credentials = await this.afAuth.auth.signInWithEmailAndPassword(email, password);
    return this.updateUserData(<any>credentials.user);
  }

  emailAndPasswordSignUp = async (email: string, password: string) => {
    const credentials = await this.afAuth.auth.createUserWithEmailAndPassword(email, password);
    return this.updateUserData(<any>credentials.user);
  }

  signOut = async () => {
    await this.afAuth.auth.signOut();
  }

  private updateUserData = ({ email, uid: id, ra: token }: Credentials) => {
    const user = { email, id, token };
  }

}