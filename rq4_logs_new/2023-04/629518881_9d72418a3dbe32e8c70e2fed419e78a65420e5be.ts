import { Injectable } from '@angular/core';
import {
  Auth,
  signOut,
  authState,
  GoogleAuthProvider,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
} from '@angular/fire/auth';
import { AngularFireAuth } from '@angular/fire/compat/auth';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root',
})
export class AuthService {
  currentActiveUser$ = authState(this.afAuth);

  constructor(
    public afAuth: Auth,
    private afProvider: AngularFireAuth,
    private router: Router
  ) {}

  async signUp(email: string, password: string) {
    await createUserWithEmailAndPassword(this.afAuth, email, password);
    return await signInWithEmailAndPassword(this.afAuth, email, password);
  }

  async signIn(email: string, password: string) {
    return await signInWithEmailAndPassword(this.afAuth, email, password);
  }

  async logout() {
    return await signOut(this.afAuth);
  }

  //providers
  googleAuth() {
    return this.authLoginGoogle(new GoogleAuthProvider());
  }

  async authLoginGoogle(provider: GoogleAuthProvider) {
    try {
      await this.afProvider.signInWithPopup(provider);
      //here redirection to the dashboard
      console.log('[LOGIN SUCCESS]');
    } catch (error) {
      console.error('[LOGIN ERROR]', error);
    }
  }
}