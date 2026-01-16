import { Injectable } from '@angular/core';
import { AngularFireObject } from '@angular/fire/database';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { Code } from '../models/code';

@Injectable({
  providedIn: 'root'
})
export class CodeService {

  codesRef: AngularFirestoreCollection<any>|any ;
  codeRef: AngularFireObject<any> | undefined;

  dbPath = '/code';

  constructor(private db: AngularFirestore) {
    this.codesRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Code> {
    return <any>this.codesRef;
  }

  AddWork(code: Code) {
    return this.codesRef?.add({...code});
  }


  UpdateWork(code: Code) {
    this.codesRef?.update({
      colid:code.colid,
      typename: code.typename,
      datailcode:code.datailcode,
      codename: code.codename,
      nickname:code.nickname,
      checked: code.checked,
})}}