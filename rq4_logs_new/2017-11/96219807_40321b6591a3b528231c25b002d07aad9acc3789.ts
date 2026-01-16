import { AuthService } from 'app/core/auth.service';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import {
  AngularFirestoreCollection,
  AngularFirestore
} from 'angularfire2/firestore';

interface DataEntry {
  $key?: string;
}

export abstract class DataAccessService<T extends DataEntry> {
  dataChange: BehaviorSubject<T[]> = new BehaviorSubject<T[]>([]);

  collection: AngularFirestoreCollection<T>;

  constructor(
    protected afs: AngularFirestore,
    protected authSVC: AuthService,
    protected additionalPath?: string
  ) {
    this.subscribeToDataChanges(additionalPath);
  }

  private subscribeToDataChanges(additionalPath: string = '') {
    this.authSVC.user.subscribe(user => {
      if (user) {
        this.collection = this.afs.collection<T>(
          `users/${user.uid}/entries${additionalPath}`
        );
        this.subscribeToSnapShotChanges(this.collection);
      }
    });
  }

  private subscribeToSnapShotChanges(collection) {
    collection.snapshotChanges().subscribe(actions => {
      const entries: T[] = [];
      actions.forEach(action => {
        entries.push(this.createInitialEntry(action));
      });
      this.dataChange.next(entries);
    });
  }

  deleteEntry(key: string) {
    return this.collection.doc(key).delete();
  }

  saveEntry(entry: T) {
    return this.collection.add(entry);
  }

  getEntry(key: string) {
    return this.collection
      .doc(key)
      .snapshotChanges()
      .map(action => {
        return this.createInitialEntry(action);
      });
  }

  private createInitialEntry(action): T {
    let entry: T;

    if (action.payload.doc) {
      entry = action.payload.doc._document.value();
      entry.$key = action.payload.doc.id;
    } else {
      entry = action.payload._document.value();
      entry.$key = action.payload.id;
    }

    return entry;
  }

  updateEntry(entry: T) {
    const key = entry.$key;
    delete entry.$key;

    this.collection.doc(key).update(entry);
  }
}