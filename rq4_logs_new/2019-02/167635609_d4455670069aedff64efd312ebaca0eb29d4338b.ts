import { Injectable } from '@angular/core';
import { AngularFireDatabase, AngularFireList } from '@angular/fire/database';


@Injectable({
  providedIn: 'root'
})
export class FirebaseService {
    lists: AngularFireList<any>;
    constructor(private afData: AngularFireDatabase) {
        afData.database.ref('/Users');
        this.lists = afData.list('Users/');
    }

    createUser(email: string, firstname: string, lastname: string) {
        this.afData.database.ref('Users/').set({
            email: email,
            firstname: firstname,
            lastname: lastname
        });
    }

    getUser() {
        this.afData.database.ref("Users/").on('value', function (snapshot) {
            var User = snapshot.val();
        });

    }
    delete() {

    }
}