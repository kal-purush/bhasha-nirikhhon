package com.capgemeni.railwayBooking.Entity;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "user-data")
public class Userinfo {
	private String _id;
	private String username;
	private String password;
	private String contact;
	private String address ;
	private String usertype;
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getContact() {
		return contact;
	}
	public void setContact(String contact) {
		this.contact = contact;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getUsertype() {
		return usertype;
	}
	public void setUsertype(String usertype) {
		this.usertype = usertype;
	}
	public Userinfo() {}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public Userinfo(String username, String password, String contact, String address, String usertype) {
		super();
		this.username = username;
		this.password = password;
		this.contact = contact;
		this.address = address;
		this.usertype = usertype;
	}
	
}