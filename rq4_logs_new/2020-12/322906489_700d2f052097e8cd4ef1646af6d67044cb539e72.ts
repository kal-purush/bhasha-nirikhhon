import { Injectable } from '@angular/core';
import { AngularFireAuth } from '@angular/fire/auth';
import { Router } from '@angular/router';

import { environment } from '../../environments/environment';

@Injectable({
    providedIn: 'root'
})
export class AuthService {
    isAdmin = false;
    isUser = false;

    constructor(private afAuth: AngularFireAuth, private router: Router) {
        this.afAuth.authState.subscribe(user => {
            if (user) {
                if (user.email === environment.adminEmail) {
                    this.isAdmin = true;
                    this.isUser = false;
                } else {
                    this.isAdmin = false;
                    this.isUser = true;
                }
            } else {
                this.isAdmin = false;
                this.isUser = false;
            }
        });
    }

    signIn(email: string, password: string, successRedirectUrl = '', errorRedirectUrl = '') {
        return this.afAuth.signInWithEmailAndPassword(email, password)
            .then(res => {
                if (res.user) {
                    if (res.user.email === environment.adminEmail) {
                        this.isAdmin = true;
                        this.isUser = false;
                    } else {
                        this.isAdmin = false;
                        this.isUser = true;
                    }
                    if (successRedirectUrl !== '') {
                        this.router.navigate([successRedirectUrl]);
                    }
                } else {
                    alert('Failed to sign in');
                    if (errorRedirectUrl !== '') {
                        this.router.navigate([errorRedirectUrl]);
                    }
                }
            })
            .catch(error => {
                console.log(error);
                if (errorRedirectUrl !== '') {
                    this.router.navigate([errorRedirectUrl]);
                }
            });
    }

    signOut() {
        return this.afAuth.signOut();
    }

}