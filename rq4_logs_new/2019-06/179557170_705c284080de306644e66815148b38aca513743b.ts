import { Injectable } from '@angular/core';
import { AngularFireDatabase, AngularFireList, AngularFireObject } from '@angular/fire/database';  // Firebase modules for Database, Data list and Single object
import {AngularFireAuth} from '@angular/fire/auth';
import { Partner } from '../study-page/partner';

@Injectable({
  providedIn: 'root'
})
export class StudyPartnerService {

  constructor(private db: AngularFireDatabase, private authService: AngularFireAuth) { }

  partners: AngularFireList<any>;
  partner: AngularFireObject<any>;

  register(partner: Partner) {
    partner.contact = this.authService.auth.currentUser.email;
    partner.userid = this.authService.auth.currentUser.uid;
    this.partners.push(partner);
  }

  getPartner(id: string) {
    this.partner = this.db.object('study-partners/' + id);
    return this.partner;
  }

  getPartners() {
    this.partners = this.db.list('study-partners');
    return this.partners;
  }

  updatePartner(partner: Partner) {
    this.partner.update(partner);
  }

  deletePartner(id: string) {
    this.partner = this.db.object('study-partners/' + id);
    this.partner.remove();
  }
}