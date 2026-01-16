package com.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.presentation.ContactDetails;
import com.presentation.ContactList;
import com.services.ContactService;

@RestController
@RequestMapping("/contacts")
public class ContactManagementController {

	@Autowired
	private ContactService contactService;

	@PostMapping("/add-contacts")
	public void addContacts(@RequestBody ContactDetails contactDetails){
		//TODO
	}

	@GetMapping("/show-contacts")
	public ContactList getContacts (){
		//TODO
		return new ContactList();
	}

	@DeleteMapping("/delete-contact")
	public void deleteContact(@RequestParam String contactNumber){
		//TODO
	}

	@GetMapping("/search-contacts")
	public ContactList searchContacts(@RequestParam String searchString){
		//TODO
		return new ContactList();
	}

	@PutMapping("/edit-contacts")
	public void updateContact(@RequestParam String contactNumber){
		//TODO
	}
}