package com.tableoneproject.tableone.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.Valid;

@Entity
@Table(name="login")
public class tableoneloginmodel {
	private int id;
	private String username;
	private String password;
	private String emailid;
	private String firstname;
	private String lastname;
	
	@Id @Column(name="id", nullable=false)
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	@Column(name="username", nullable=false)
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	@Column(name="password", nullable=false)
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	@Column(name="emailid", nullable=false)
	public String getEmailid() {
		return emailid;
	}
	public void setEmailid(String emailid) {
		this.emailid = emailid;
	}
	@Column(name="firstname", nullable=false)
	public String getFirstname() {
		return firstname;
	}
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	@Column(name="lastname", nullable=false)
	public String getLastname() {
		return lastname;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public tableoneloginmodel(int id, String username, String password, String emailid, String firstname,
			String lastname) {
		super();
		this.id = id;
		this.username = username;
		this.password = password;
		this.emailid = emailid;
		this.firstname = firstname;
		this.lastname = lastname;
	}
	public tableoneloginmodel() {
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString() {
		return "tableoneloginmodel [id=" + id + ", username=" + username + ", password=" + password + ", emailid="
				+ emailid + ", firstname=" + firstname + ", lastname=" + lastname + "]";
	}
	
	
	
	
}