import { Injectable } from '@angular/core';
import {Storage, LocalStorage} from 'ionic-angular';

/*
  Generated class for the LocalStorageProvider provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class LocalStorageProvider {
    private local;

    constructor() {
        this.local = new Storage(LocalStorage);
    }

    getFromLocal(key) {
        return this.local.get(key).then((value) => {
            return value;
        });
    }

    removeFromLocal(key) {
        this.local.remove(key);
    }

    setToLocal(key, value) {
        this.local.set(key, value);
    }
}