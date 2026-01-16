package com.bridgelabz.addressbookmanagementsystem;

import java.util.ArrayList;
import java.util.Scanner;

public class AddressBook {
    ArrayList<Contact> arrayList = new ArrayList<>();
    public void addContacts() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Firstname");
        String firstName = sc.next();
        System.out.println("Enter Lastname");
        String lastname = sc.next();
        System.out.println("Enter city");
        String city = sc.next();
        System.out.println("Enter state");
        String state = sc.next();
        System.out.println("Enter Zipcode");
        int zipcode = sc.nextInt();
        System.out.println("mobile Number");
        String mobileNumber = sc.next();
        System.out.println("email id");
        String emailId = sc.next();
        Contact contact = new Contact();
        contact.setFirstName(firstName);
        contact.setLastName(lastname);
        contact.setCity(city);
        contact.setState(state);
        contact.setEmailId(emailId);
        contact.setMobileNumber(mobileNumber);
        contact.setZipCode(zipcode);
        arrayList.add(contact);
    };
}