import { AngularFireAuth } from 'angularfire2/auth';

import { Injectable } from '@angular/core';

import { Firebase } from '@ionic-native/firebase';
import { AngularFirestore } from 'angularfire2/firestore';
import { Platform } from 'ionic-angular';


@Injectable()
export class FcmProvider {

  constructor(private afAuth: AngularFireAuth,public firebaseNative: Firebase,
  public afs: AngularFirestore, public platform: Platform) {}



  async getToken(){
    let token;
    if(this.platform.is('android')){
      token =  await this.firebaseNative.getToken();
    }
    else if (this.platform.is('ios')) {
      token = await this.firebaseNative.getToken();
      await this.firebaseNative.grantPermission();
    }
    else{
      token = 'not available'
    }
    return this.saveTokenToFirestore(token);
  }

  // Save the token to firestore
  private saveTokenToFirestore(token) {
    if(!token){
      return;
    }
    let userUid;
    if(this.afAuth.auth.currentUser){
      userUid = this.afAuth.auth.currentUser.uid;
    }
    else{
      userUid = 'unavailable';
    }
    const devicesRef = this.afs.collection('devices');
    const docData = {
      token,
      userId: userUid
    };
    return devicesRef.doc(token).set(docData);
  }


  listenToNotifications() {
    return this.firebaseNative.onNotificationOpen();
  }
}