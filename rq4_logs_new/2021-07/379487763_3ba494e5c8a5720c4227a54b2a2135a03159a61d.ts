import { Injectable } from '@angular/core';
import { AngularFireList, AngularFireObject } from '@angular/fire/database';
import { Notice } from 'modules/notice/models/notice'
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';

@Injectable({
  providedIn: 'root'
})
export class NoticeService {
  noticesRef: AngularFirestoreCollection<any> | undefined;
  noticeRef: AngularFireObject<any> | undefined;

  dbPath = '/notice';

  constructor(private db: AngularFirestore) {
    this.noticesRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Notice> {
    return <any>this.noticesRef;
  }

  AddNotice(notice: Notice) {
    return this.noticesRef?.add({...notice});
  }

  /*
  GetNotice(id: string) {
    this.noticeRef = this.db.object('notices-list/' + id);
    return this.noticeRef
  }

  GetNoticesList() {
    this.noticesRef = this.db.list('notices-list');
    return this.noticesRef
  }

  */

  UpdateNotice(notice: Notice) {
    this.noticeRef?.update({
      type: notice.type,
      title: notice.title,
      writer: notice.writer,
      date: notice.date,
      content: notice.content,
      addfile: notice.addfile,
      modify: notice.modify,
      checked: notice.checked
    })
  }

  /*
  DeleteNotice(id: string) {
    this.noticeRef = this.db.object('notices-list/' + id);
    this.noticeRef.remove();
  }
  */
}