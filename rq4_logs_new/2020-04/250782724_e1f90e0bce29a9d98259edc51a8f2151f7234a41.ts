import { Injectable } from '@angular/core';
import { Auth } from 'aws-amplify';
import { EventEmitterService } from '../event-emitter/event-emitter.service';
import { Subject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  constructor() { }

  private customSubject = new Subject<any>();
  customObservable = this.customSubject.asObservable();

  // Service message commands
  callComponentMethod() {
    this.customSubject.next();
  }

  signIn(userN, passW) {

    const user = {
     username: userN,
     password: passW
   };

    Auth.signIn(user).then( user => {
     console.log(user);
     this.callComponentMethod();
   }).catch(err => {
     console.log(err);
   });
  }
}