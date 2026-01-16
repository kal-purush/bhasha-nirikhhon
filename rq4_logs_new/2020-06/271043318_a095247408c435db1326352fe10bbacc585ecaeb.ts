import { Injectable } from '@angular/core';
import {AngularFirestore, AngularFirestoreCollection, DocumentChangeAction} from '@angular/fire/firestore';
import {Rating} from './rating';
import {Observable} from 'rxjs';
import {map} from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class RatingService {

  ratingCollection: AngularFirestoreCollection<Rating>;

  static prepare(rating: Rating): Rating{
    const copy = {...rating};
    delete copy.id;
    copy.points = copy.points || null;
    copy.description = copy.description || null;
    return copy;
  }

  constructor(private afs: AngularFirestore) { }


  persist(rating: Rating, userId: string){
    this.ratingCollection = this.afs.collection<Rating>('user/' + userId + '/ratings');
    this.ratingCollection.add(RatingService.prepare(rating));
  }

  findAll(userId: string){
    this.ratingCollection = this.afs.collection<Rating>('user/' + userId + '/ratings');
    const changeActions: Observable<DocumentChangeAction<Rating>[]> = this.ratingCollection.snapshotChanges();
    return changeActions.pipe(
        map(actions => actions.map(a => {
          const data = a.payload.doc.data();
          data.id = a.payload.doc.id;
          return {...data} as Rating;
        }))
    );
  }


}