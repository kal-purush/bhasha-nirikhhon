package ru.test.tbook;

import ru.test.tbook.domain.Contact;

import java.util.List;

public interface AddressBook {

    List<Contact> lookup(String namePrefix);

    void add(Contact contact);
}