import { AngularFireList, AngularFireDatabase } from 'angularfire2/database';
import { Injectable } from '@angular/core';
import { Contact } from './contact.model';

@Injectable()
export class ContactService {

  contactList: AngularFireList<any>;
  selectedContact: Contact = new Contact();

  constructor(private firebase: AngularFireDatabase) { }

  getData() {
    this.contactList = this.firebase.list('contact');
    return this.contactList;
  }

  insertContact(contact: Contact) {
    this.contactList = this.firebase.list('contact');
    const newPostRef = this.contactList.push({
      name: contact.name,
      surname: contact.surname,
      email: contact.email,
      phone: contact.phone,
      textArea: contact.textArea
    });
  }

  deleteContact($key: string) {
    this.contactList.remove($key);
  }

}