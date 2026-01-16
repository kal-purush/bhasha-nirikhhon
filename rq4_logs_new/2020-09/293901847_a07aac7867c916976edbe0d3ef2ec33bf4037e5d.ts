import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import {
  AngularFirestore,
  AngularFirestoreCollection,
} from '@angular/fire/firestore';
import { Observable } from 'rxjs';
import { Hrana } from '../models/hrana-unos';


@Injectable({
  providedIn: 'root'
})
export class HranaService {

  hranaCollection: AngularFirestoreCollection<Hrana>;
  hrana: Observable<Hrana[]>;

  constructor(public afs: AngularFirestore) {

    this.hranaCollection = this.afs.collection('unos_hrane', (ref) => {
      return ref.orderBy('naziv', 'asc');
    });

    this.hrana = this.hranaCollection.snapshotChanges().map((newData) => {
      return newData.map((b) => {
        const hranaData = b.payload.doc.data() as Hrana;
        // hranaData.id = b.payload.doc.id;
        return hranaData;
      });
    });
  }

  getFood() {
    return this.hrana;
  }
  addHrana(hrana: Hrana) {
    this.hranaCollection.add(hrana).then(() => {
      alert('Uspjesan unos!');
    });
  }

}