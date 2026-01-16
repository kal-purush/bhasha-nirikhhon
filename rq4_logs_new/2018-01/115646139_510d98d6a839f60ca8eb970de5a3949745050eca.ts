import { Injectable } from '@angular/core';
import {AngularFireDatabase, AngularFireList} from 'angularfire2/database';

@Injectable()
export class SearchService {

  constructor(private database: AngularFireDatabase) { }

  getUser(start, end): AngularFireList<any> {
    return this.database.list('/users', ref =>
        ref.limitToFirst(10).endAt(end).startAt(start).orderByChild('firstName')
      // query: {
      //   startAt: start,
      //   endAt: end,
      //   limitToFirst: 10,
      //   orderByChild: 'firstName' + 'lastName'
      // }


    );
  }

}