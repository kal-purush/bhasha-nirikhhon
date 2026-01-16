import { Injectable } from '@angular/core';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class PlayerPointsService {

  playerPointsCollection: AngularFirestoreCollection<PlayerPoints>;

  constructor(private afs: AngularFirestore) {
    this.playerPointsCollection = this.afs.collection('playerPoints');
  }

  getPlayerPoint(uid: string, seasonId: string): Observable<PlayerPoints> {
    const playerPoint = this.afs.collection('playerPoints', ref =>
      ref.where('seasonId', '==', seasonId).where('playerId', '==', uid));

    return playerPoint.doc<PlayerPoints>(seasonId + uid).valueChanges();
  }

  getTotalPlayerPoints(uid: string): Observable<PlayerPoints[]> {
    const playerPoints: AngularFirestoreCollection<PlayerPoints> = this.afs.collection('playerPoints', ref =>
      ref.where('playerId', '==', uid));

    return playerPoints.snapshotChanges().pipe(
      map((playerPts) => playerPts.map(item => item.payload.doc.data()))
    );
  }

  getTeamPoints(teamId: string, seasonId: string): Observable<PlayerPoints[]> {
    const playerPoints: AngularFirestoreCollection<PlayerPoints> = this.afs.collection('playerPoints', ref =>
      ref.where('teamId', '==', teamId));

      return playerPoints.snapshotChanges().pipe(
        map((playerPts) => playerPts.map(item => item.payload.doc.data()))
      );
  }

  getSeasonExp(uid: string, seasonId: string): Observable<number> {
    return this.getPlayerPoint(uid, seasonId).pipe(
      map((playerPoint: PlayerPoints) => playerPoint ? playerPoint.totalPoints : 0)
    );
  }

  getTotalExp(uid: string): Observable<number> {
    return this.getTotalPlayerPoints(uid).pipe(map((playerPoints) => {
      let totalPoints = 0;

      playerPoints.forEach(playerPoint => {
        totalPoints += playerPoint.totalPoints;
      });

      return totalPoints;
    }));
  }

}