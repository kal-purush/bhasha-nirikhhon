package by.ras.impl;

import by.ras.BaseTest;
import by.ras.ContactService;
import by.ras.entity.Occupation;
import by.ras.entity.Role;
import by.ras.entity.Sex;
import by.ras.entity.particular.Contact;

import by.ras.entity.particular.User;
import by.ras.repository.UserRepository;
import lombok.extern.log4j.Log4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;

import static org.junit.Assert.*;

@Log4j
@Transactional
public class ContactServiceTest extends BaseTest{

    @Autowired
    ContactService contactService;
    @Autowired
    UserRepository userRepository;

    @Test
    public void addContact() throws Exception {
        User user = User.builder()
                .name("User")
                .surname("User")
                .login("testUser")
                .password("q1Q")
                .sex(Sex.MALE.name())
                .occupation(Occupation.UNEMPLOYED.name())
                .role(Role.CLIENT.name())
                .date(new Date(System.currentTimeMillis()))
                .build();
        user = userRepository.saveAndFlush(user);
        Contact contact = Contact.builder()
                .user(user)
                .country("Belarus")
                .city("Minsk")
                .street("Marx")
                .houseNumber("10")
                .phoneNumber("375291234567")
                .build();
        contactService.addContact(contact);
        contact = contactService.addContact(contact);
        Contact foundContact = contactService.findById(contact.getId());

        assertEquals(contact, foundContact);
    }

    @Test
    public void editContact() throws Exception {
        User user = userRepository.findByLogin("user");
        long contactId = user.getContact().getId();
        Contact newContact = new Contact(user, "Test", "Test", "Test", "000", "010101010101");
        newContact.setId(contactId);
        contactService.editContact(newContact);
        Contact foundContact = userRepository.findByLogin("user").getContact();

        assertEquals(newContact.getCountry(), foundContact.getCountry());
        assertEquals(newContact.getCity(), foundContact.getCity());
        assertEquals(newContact.getStreet(), foundContact.getStreet());
        assertEquals(newContact.getHouseNumber(), foundContact.getHouseNumber());
        assertEquals(newContact.getPhoneNumber(), foundContact.getPhoneNumber());


    }

}