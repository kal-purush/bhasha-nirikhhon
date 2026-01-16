import { Injectable } from '@angular/core';
import { User } from '../interfaces';
import { Observable } from "rxjs/Observable";
import { fireBaseConfig, databasePaths } from '../configs';
import firebase from 'firebase';

@Injectable()
export class DatabaseProvider {
    private _db: firebase.database.Database;
    private _usersRef: firebase.database.Reference;

    constructor() {
        if (!firebase.apps.length) {
            firebase.initializeApp(fireBaseConfig);
        }

        this._db = firebase.database();

        this._usersRef = this._db.ref(databasePaths.usersPath);
    }

    public getOrg() {
        const uid = '4f2jsiyTLTauwoxcX78tPC7ObEE3';

        this._usersRef.child(uid).on('value', (snapshot) => {
            console.log(snapshot.val());
        });
    }
}