package ru.geekbrains.java2.les3;


import java.util.List;
import java.util.ArrayList;

public class PhoneBook {

  private List<Contact> contactListBook;

  public PhoneBook(){
    this.contactListBook = new ArrayList<>();
  }
  public void addContact(Contact contact){
    this.contactListBook.add(contact);

  }
  public String getContact (String name) {
    String contactInformacion = name ;

    for (Contact contact: this.contactListBook) {
      if (contact.getName().equals(name)){
        contactInformacion = contactInformacion + " " + contact.getPhoneNumber();
      }

    }
    return  contactInformacion;

  }
}







































































































































