import { Injectable } from '@angular/core';
import {
  AngularFirestoreDocument,
  AngularFirestore
} from '@angular/fire/firestore';
import { AuthService } from 'src/app/core/auth.service';

export interface Distributor {
  name: string;
  latitude: number;
  longitude: number;
}

@Injectable({
  providedIn: 'root'
})
export class DistributorService {
  // TODO : edit table rows
  constructor(private auth: AuthService, private afs: AngularFirestore) {}

  getDistributors() {
    const uid = this.auth.user.uid;
    const distributorRef: AngularFirestoreDocument<any> = this.afs.doc(
      `distributors/${uid}`
    );

    return distributorRef.get();
  }
  addDistributor(values) {
    return this.updateDistributorData(this.auth.user.uid, values);
  }

  updateDistributorData(uid, data): Promise<Distributor> {
    return new Promise((resolve, reject) => {
      const distributorRef: AngularFirestoreDocument<any> = this.afs.doc(
        `distributors/${uid}`
      );

      const newDistributor: Distributor = {
        name: data.name,
        longitude: parseFloat(data.longitude),
        latitude: parseFloat(data.latitude)
      };

      return distributorRef
        .set({ [newDistributor.name]: newDistributor }, { merge: true })
        .then(res => resolve(newDistributor))
        .then(err => reject(err));
    });
  }
}