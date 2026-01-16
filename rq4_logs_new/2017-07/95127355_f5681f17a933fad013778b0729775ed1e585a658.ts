import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import { AngularFireDatabase, FirebaseListObservable, FirebaseObjectObservable} from 'angularfire2/database';
import firebase from 'firebase/app'
import { AngularFireAuth } from 'angularfire2/auth';
import {Observable} from 'rxjs/Observable';
/*
  Generated class for the UserInfoProvider provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular DI.
*/
@Injectable()
export class UserInfo {
  private userToken;
  private path : string;
  constructor(
    private db : AngularFireDatabase,
    private afAuth: AngularFireAuth
  )
  {
    this.path = '/user/' + this.afAuth.auth.currentUser.uid+ '/info';
  }
  getUserToken()
  {
    return this.afAuth.auth.currentUser.uid;
  }
  getUserInfo()
  {
    this.path = '/user/' + this.afAuth.auth.currentUser.uid+ '/info';
    return this.db.object(this.path);
    /*return new Observable(observer => {
      this.getUserToken().subscribe((id) =>{
        let path : string = '/user/' + id.uid + '/info';
        observer.next(this.db.object(path));
      });
    });*/
  }
  getUserEmail()
  {
    return this.afAuth.auth.currentUser.email;
  }
  changeUserData(firstName, lastName)
  {
    let usInfo : FirebaseObjectObservable <any>;
    usInfo = this.db.object(this.path);
    usInfo.update(
      {
        firstName : firstName,
        lastName : lastName
      });
  }
}