import { Injectable } from '@angular/core';
import { AngularFireObject } from '@angular/fire/database';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { Company } from '../models/company';

@Injectable({
  providedIn: 'root'
})
export class CompanyService {

  companysRef: AngularFirestoreCollection<any>|any ;
  companyRef: AngularFireObject<any> | undefined;

  dbPath = '/company';

  constructor(private db: AngularFirestore) {
    this.companysRef = db.collection(this.dbPath);
   }

  getAll(): AngularFirestoreCollection<Company> {
    return <any>this.companysRef;
  }

  AddCompany(company: Company) {
    return this.companysRef?.add({...company});
  }


  UpdateCompany(company: Company) {
    this.companysRef?.update({
      company:company.company,
      ceo: company.ceo,
      number:company.number,
      checked: company.checked,
})}}