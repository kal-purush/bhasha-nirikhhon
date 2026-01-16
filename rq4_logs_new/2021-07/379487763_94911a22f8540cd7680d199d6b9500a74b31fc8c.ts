import { Injectable } from '@angular/core';
import { AngularFireList, AngularFireObject } from '@angular/fire/database';
import { Member } from 'modules/management/models/member';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';

@Injectable({
  providedIn: 'root'
})
export class MemberService {

  membersRef: AngularFirestoreCollection<any> | undefined;
  memberRef: AngularFireObject<any> | undefined;

  dbPath = '/member';

  constructor(private db: AngularFirestore) {
    this.membersRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Member> {
    return <any>this.membersRef;
  }

  AddNotice(member: Member) {
    return this.membersRef?.add({...member});
  }


  UpdateMember(member: Member) {
    this.membersRef?.update({
      type: member.type,
      title: member.title,
      writer: member.writer,
      date: member.date,
      content: member.content,
      addfile: member.addfile,
      modify: member.modify,
      checked: member.checked
    })
  }