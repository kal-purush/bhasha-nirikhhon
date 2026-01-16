import { Injectable } from '@angular/core';
import {Storage, SqlStorage} from 'ionic-angular';

var PouchDB = require('pouchdb');

/*
  Generated class for the PouchService provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class PouchService {
    private db;

    constructor() {
        this.db = new PouchDB('cheers', { adapter: 'websql' });
    }

    /**
     * Adds the document to the database
     */
    add(document) {
        return this.db.post(document);
    }
}
