package com.session.helper;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;

@Entity
public class Contact {
	@Id String  ID;
	
	@Index String Email;
	@Index String Name;
	@Index String Password;

	// When getting the fields from form action
	public Contact(String email, String name, String password) {
		// System.out.println("hi");
		this.ID=email;
		this.Email = email;
		this.Name = name;
		this.Password = password;
	}

	public Contact(String email, String name) {
		this.ID=email;
		this.Email = email;
		this.Name = name;
	}

	public Contact() {
		// TODO Auto-generated constructor stub
	}

}