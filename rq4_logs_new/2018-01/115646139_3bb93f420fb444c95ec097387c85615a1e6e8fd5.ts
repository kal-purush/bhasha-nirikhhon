import { Injectable } from '@angular/core';
import {AngularFireDatabase} from 'angularfire2/database';

@Injectable()
export class PostService {

  constructor(private database: AngularFireDatabase) { }

  getPostsByKey(key: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.database.list('posts').snapshotChanges().map(actions => {
        console.log('actions', actions);
        return actions.map(action => ({ key: action.key, ...action.payload.val() }));
      }).subscribe(postsList => {
        console.log('postsList', postsList);
        for (let i = 0; i < postsList.length; i++) {
          if (postsList[i]['key'] === key) {
            resolve(postsList[i]);
          }
        }
        reject('no posts');
      });
    });
  }
  getPostsLengthByKey(key: string): Promise<any> {

    return new Promise((resolve, reject) => {
      this.database.list('posts').snapshotChanges().map(actions => {
        console.log('actions', actions);
        return actions.map(action => ({ key: action.key, ...action.payload.val() }));
      }).subscribe(postsList => {
        console.log('postsList', postsList);
        for (let i = 0; i < postsList.length; i++) {
          if (postsList[i]['key'] === key) {
            let size = -1;
            for (const prop in postsList[i]) {
              if (postsList[i].hasOwnProperty(prop)) {
                size++;
              }
            }
            resolve(size);
          }
        }
        reject('no posts');
      });
    });
  }
}