package com.example.Smart_contact_manager.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.Smart_contact_manager.presentation.ContactDetail;
import com.example.Smart_contact_manager.presentation.ContactList;
import com.example.Smart_contact_manager.services.ContactService;

@RestController
@RequestMapping("/contacts")
public class ContactManagementController {

	@Autowired
	private ContactService contactService;

	@PostMapping("add")
	public String addContacts(@RequestBody ContactDetail request){
		try {
			contactService.addContactDetails(request);
			return "contact added successfully !";
		} catch (Exception e) {
			return "contact already exist !";
		}
	}

	@GetMapping("show-contacts")
	public ContactList getContacts (){
		var contactList=contactService.getContactList();
		return contactList;
	}

	@DeleteMapping("delete-contact")
	public String  deleteContact(@RequestParam String contactNumber){
		contactService.deleteContact(contactNumber);
		return "contact deleted successfully !";
	}

	@GetMapping("search-contacts")
	public ContactList searchContacts(@RequestParam String searchString){
		//TODO
		return new ContactList();
	}

	@PutMapping("edit-contacts")
	public String updateContact(@RequestBody ContactDetail request){
		contactService.updateContact(request);
		return "contact updated successfully";
	}

	@GetMapping("health-check")
	public String getHealthStatus(){
		return "up and running";
	}
}