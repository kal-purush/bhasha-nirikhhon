import {AngularFirestore, AngularFirestoreCollection} from '@angular/fire/firestore';
import {Observable} from 'rxjs';
import {Item} from '../../services/user.service';
import {AngularFireDatabase} from '@angular/fire/database';
import {AngularFireAuth} from '@angular/fire/auth';
import {FirebaseListObservable} from '@angular/fire/database-deprecated';

export abstract class UserInformation {
  usersCollection: AngularFirestoreCollection;
  items: FirebaseListObservable; // list of objects
  userId: string;
  userFavs;
  public constructor(public afs: AngularFirestore,
                     public db: AngularFireDatabase,
                     private afAuth: AngularFireAuth) {
    this.afAuth.authState.subscribe(user => {
      if (user) {
        this.userId = user.uid;
      }
    });

  }

  public isUser(): boolean {
    const authCondition = sessionStorage.getItem('auth');
    return authCondition === 'true';
  }

  addItemToCart(data) {
    this.items = this.db.list(`users/${this.userId}/cart`);
    this.items.push(data);
  }

  addItemToWishlist(data) {
    this.items = this.db.list(`users/${this.userId}/wishlist`);
    this.items.push(data);
  }

  getItemsList() {
    const relative = this.db.object(`users/${this.userId}`).valueChanges();
    relative.subscribe(data => {
      if (this.userId) {
        this.userFavs = data;
      } else {
        console.log('no user');
      }
    });
  }
}