package com.example.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.springframework.boot.test.context.SpringBootTest;

import com.fasterxml.jackson.annotation.JsonBackReference;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

@SpringBootTest(classes = Contact.class)
class ContactTests {
	private int testContactId = 123;
	private String testContactName = "John Test";
	private String testContactEmail = "testEmail@test.com";
	private LocalDate testContactDOB = LocalDate.now();
	private String testContactPhone = "727-555-1212";
	
	private User testUser = new User(12, "Joey User", "password1", "Seller", "JoeyU@test.com");
	private Contact testContact = new Contact(testContactId, testContactName, testContactEmail, testContactDOB, testContactPhone,testUser);
	private int testContactIdToUpdate = 321;
	private String testContactNameToUpdate = "Jim Test";
	private String testContactEmailToUpdate = "testEmail@yahoo.com";
	private LocalDate testContactDOBToUpdate = testContactDOB.withYear(2020);
	private String testContactPhoneToUpdate = "813-555-1212";
	
	
	
	
	@Test
	void testSetGetName() {	
		assertEquals(testContactName, testContact.getContactName());
		testContact.setContactName(testContactNameToUpdate);
		assertEquals(testContactNameToUpdate, testContact.getContactName());
	}
	
	@Test
	void testSetGetEmail() {
		assertEquals(testContactEmail, testContact.getContactEmail());
		testContact.setContactEmail(testContactEmailToUpdate);
		assertEquals(testContactEmailToUpdate, testContact.getContactEmail());
	}
	
	@Test
	void testSetGetDOB() {
		assertEquals(testContactDOB, testContact.getContactDOB());
		testContact.setContactDOB(testContactDOBToUpdate);
		assertEquals(testContactDOBToUpdate, testContact.getContactDOB());
	}
	
	@Test
	void testSetGetPhone() {
		assertEquals(testContactPhone, testContact.getContactPhone());
		testContact.setContactPhone(testContactPhoneToUpdate);
		assertEquals(testContactPhoneToUpdate, testContact.getContactPhone());
	}
	
	@Test
	void testSetGetContactId() {
		assertEquals(testContactId, testContact.getContactId());
		testContact.setContactId(testContactIdToUpdate);
		assertEquals(testContactIdToUpdate, testContact.getContactId());
	}
	
	@Test
	void testToStringContact() {
		String contactString ="Contact [contactId=321, contactName=Jim Test, email=testEmail@yahoo.com, dob=" + testContactDOBToUpdate + ", contactPhone=813-555-1212]";
		assertEquals(contactString, testContact.toString());
	}

}