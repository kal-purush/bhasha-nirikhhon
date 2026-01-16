import { Injectable } from '@angular/core';
import { AngularFireObject } from '@angular/fire/database';
import { AngularFirestore, AngularFirestoreCollection } from '@angular/fire/firestore';
import { Company } from '@modules/management/models/company';

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

  AddMember(company: Company) {
    return this.companysRef?.add({...company});
  }


  UpdateMember(company: Company) {
    this.companysRef?.update({
      name:company.name,
      boss:company.boss,
      address: company.address,
      rname: company.rname,
      contact: company.contact,
      businpariod:company.businpariod,
      checked: company.checked,
  })
  }
}