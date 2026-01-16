import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';

//import { AuthGuardService } from '../service/auth-guard.service';
import { AuthService } from './../service/auth-service.service';

import { AngularFireAuth } from '@angular/fire/auth';
import { UserService } from './../service/user.service';
import { Router } from '@angular/router';

import { auth, User } from 'firebase';

@Component({
  selector: 'app-register',
  templateUrl: './register.component.html',
  styleUrls: ['./register.component.css']
})
export class RegisterComponent implements OnInit {
  registerForm: FormGroup;
  loginForm: FormGroup;
  result;
  user;
  message;
  userProfil: User;
  authStatus: boolean;
  isAuth: boolean;


  constructor(private authService: AuthService/*authGuardService: AuthGuardService*/, private fb: FormBuilder, private afAuth: AngularFireAuth, private userService: UserService, private router: Router) { }

  ngOnInit(): void {
  this.registerForm = this.fb.group({
      email: ['', Validators.email],
      password: ['',[Validators.required, Validators.minLength(6)]]
    });

    this.loginForm = this.fb.group({
      email: ['', Validators.email],
      password: ['',[Validators.required, Validators.minLength(6)]]
    });

    this.afAuth.authState.subscribe((userProfil) => {
      this.userProfil = userProfil;
    });

    //this.authStatus = this.authGuardService.isAuth;


    console.log("user profil : ", this.userProfil);

}

async register() {
  if (!this.registerForm.valid) {
    console.log('grrr');      
    return;
  } 
  console.log('register', this.registerForm.value); 
  //this.result = await this.afAuth.createUserWithEmailAndPassword(this.registerForm.value.email,this.registerForm.value.password);
  this.result = await this.authService.signIn(this.registerForm.value.email,this.registerForm.value.password);
  console.log('register / result ', this.result);
  
  this.registerForm.reset();
  if( this.result && this.result.user) {
    const userCreated = await this.userService.createUser(this.result.user);
    console.log('userCreated', userCreated);
    this.result = null;
  }
}

async login() {
  if (!this.loginForm.valid) {
    console.log(':(');
    return;
  }
  try {
    this.message = '';
    console.log('login', this.loginForm.value);
    //const email = this.this.loginForm.value.email;
    //const password = this.this.loginForm.value.password;
    const { email, password} = this.loginForm.value;
    this.user = await this.authService.login(email, password); //appel à la méthode
    //this.user = await this.afAuth.signInWithEmailAndPassword(email, password); //dans le resultat, on met le await et sans oublier de mettre le async à côté de la fonction 
    if (this.user){
      this.router.navigate(['dashboard']);    
    }
  } catch (error) {
    this.message = error.message;
  }
}
onActif(){
  this.isAuth = true;
}
onSignOut() {
  this.isAuth = false;
}

/*authStatus: boolean;

  constructor(private authService: AuthService, private router: Router) { }

  ngOnInit(): void {
    this.authStatus = this.authService.isAuth;
  }

  onSignIn() {
    this.authService.signIn().then(
      () => {
        console.log('Sign in successful!');
        this.authStatus = this.authService.isAuth;
        this.router.navigate(['dashboard']);
      }
    );
  }

  onSignOut() {
    this.authService.signOut();
    this.authStatus = this.authService.isAuth;
  }*/

}