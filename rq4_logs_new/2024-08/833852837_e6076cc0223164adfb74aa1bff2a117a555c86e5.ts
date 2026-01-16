import { inject, Injectable } from '@angular/core';
import { Auth, GoogleAuthProvider, signInWithPopup, signOut, User, user } from '@angular/fire/auth';
import { Router } from '@angular/router';

@Injectable({
    providedIn: 'root'
})
export class AuthService {

    router: Router = inject(Router);

    auth: Auth = inject(Auth);
    user$ = user(this.auth);
    currentUser: User | null = this.auth.currentUser;
    private provider = new GoogleAuthProvider();

    constructor() {
        this.user$.subscribe((aUser: User | null) => {
            this.currentUser = aUser;
        });

        this.user$.pipe().subscribe(user => {
            console.log('aSvc ctor user sub: ', user);
        });
    }

    login() {
        signInWithPopup(this.auth, this.provider).then((result) => {
            console.log('aSvc l login result: ', result);
            const credential = GoogleAuthProvider.credentialFromResult(result);
            this.router.navigate(['/', 'home']);
            return credential;
        })
    }

    logout() {
        signOut(this.auth).then(() => {
            this.router.navigate(['/', 'login'])
            console.log('signed out');
        }).catch((error) => {
            console.log('sign out error: ' + error);
        })
    }
}