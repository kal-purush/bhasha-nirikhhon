import { Injectable } from '@angular/core';
import {AngularFirestore, AngularFirestoreCollection, DocumentChangeAction} from "@angular/fire/firestore";
import {Trail} from "../trail";
import {Observable} from "rxjs";
import {map} from "rxjs/operators";

@Injectable({
  providedIn: 'root'
})
export class UserTrailsService {
    private trailsInterestedRef: AngularFirestoreCollection<Trail>;
    private trailsCompletedRef: AngularFirestoreCollection<Trail>;

  constructor(private db: AngularFirestore) {
    this.trailsInterestedRef = this.db.collection<Trail>(`interestedTrails`);
    this.trailsCompletedRef = this.db.collection<Trail>(`completedTrails`);
  }


  getInterestedTrailsObservable(): Observable<Trail[]> {
    return this.trailsInterestedRef.snapshotChanges()
      .pipe(
        map((items: DocumentChangeAction<Trail>[]): Trail[] => {
          return items.map((item: DocumentChangeAction<Trail>): Trail => {
            return {
              id: item.payload.doc.data().id,
              name: item.payload.doc.data().name,
              type: item.payload.doc.data().type,
              summary: item.payload.doc.data().summary,
              difficulty: item.payload.doc.data().difficulty,
              stars: item.payload.doc.data().stars,
              starVotes: item.payload.doc.data().starVotes,
              location: item.payload.doc.data().location,
              url: item.payload.doc.data().url,
              imgSqSmall: item.payload.doc.data().imgSqSmall,
              imgSmall: item.payload.doc.data().imgSmall,
              imgSmallMed: item.payload.doc.data().imgSmallMed,
              imgMedium: item.payload.doc.data().imgMedium,
              length: item.payload.doc.data().length,
              ascent: item.payload.doc.data().ascent,
              descent: item.payload.doc.data().descent,
              high: item.payload.doc.data().high,
              low: item.payload.doc.data().low,
              longitude: item.payload.doc.data().longitude,
              latitude: item.payload.doc.data().latitude,
              conditionStatus: item.payload.doc.data().conditionStatus,
              conditionDetails: item.payload.doc.data().conditionDetails,
              conditionDate: item.payload.doc.data().conditionDate
            }
          })
        })
      )
  }

  getCompletedTrailsObservable(): Observable<Trail[]> {
    return this.trailsCompletedRef.snapshotChanges()
      .pipe(
        map((items: DocumentChangeAction<Trail>[]): Trail[] => {
          return items.map((item: DocumentChangeAction<Trail>): Trail => {
            return {
              id: item.payload.doc.data().id,
              name: item.payload.doc.data().name,
              type: item.payload.doc.data().type,
              summary: item.payload.doc.data().summary,
              difficulty: item.payload.doc.data().difficulty,
              stars: item.payload.doc.data().stars,
              starVotes: item.payload.doc.data().starVotes,
              location: item.payload.doc.data().location,
              url: item.payload.doc.data().url,
              imgSqSmall: item.payload.doc.data().imgSqSmall,
              imgSmall: item.payload.doc.data().imgSmall,
              imgSmallMed: item.payload.doc.data().imgSmallMed,
              imgMedium: item.payload.doc.data().imgMedium,
              length: item.payload.doc.data().length,
              ascent: item.payload.doc.data().ascent,
              descent: item.payload.doc.data().descent,
              high: item.payload.doc.data().high,
              low: item.payload.doc.data().low,
              longitude: item.payload.doc.data().longitude,
              latitude: item.payload.doc.data().latitude,
              conditionStatus: item.payload.doc.data().conditionStatus,
              conditionDetails: item.payload.doc.data().conditionDetails,
              conditionDate: item.payload.doc.data().conditionDate
            }
          })
        })
      )
  }

  getTrailObservable(id:number): Observable<Trail> {
    return this.db.doc<Trail>(`trails/${id}`).valueChanges();
  }

  saveInterestedTrail(trail: Trail) {
    return this.trailsInterestedRef.add(trail)
      .then(_ => console.log('Success on set'));
  }

  saveCompletedTrail(trail: Trail) {
    return this.trailsCompletedRef.add(trail)
      .then(_ => console.log('Success on set'));
  }



}