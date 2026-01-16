import { Injectable } from '@angular/core';
import { AngularFireObject } from '@angular/fire/database';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { Memo } from '@modules/navigation/models/memo';

@Injectable({
  providedIn: 'root'
})
export class MemoService {

  memosRef: AngularFirestoreCollection<any>|any ;
  memoRef: AngularFireObject<any> | undefined;

  dbPath = '/memo';

  constructor(private db: AngularFirestore) {
    this.memosRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Memo> {
    return <any>this.memosRef;
  }

  AddMemo(memo: Memo) {
    return this.memosRef?.add({...memo});
  }


  UpdateMemo(memo: Memo) {
    this.memosRef?.update({
      content:memo.content,
      send:memo.send,
      date:memo.date,
      management:memo.management,
      checked:memo.checked,
})}}