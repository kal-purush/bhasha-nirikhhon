import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as firebase from 'firebase-admin';

@Injectable()
export class DataService extends firebase.firestore.Firestore implements OnModuleInit, OnModuleDestroy {

  constructor() {
    super();
  }

  public async onModuleDestroy() {
    // await this.$disconnect();
  }

  public async onModuleInit() {
    // const app = firebase.initializeApp({
    //   credential: firebase.credential.applicationDefault()
    // }, 'firestoreApp');

    // More specific init needed?
    Logger.log('Firestore client connected'); // Example of logging
  }

}