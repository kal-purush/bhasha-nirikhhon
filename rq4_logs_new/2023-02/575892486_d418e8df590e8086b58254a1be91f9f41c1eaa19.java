package com.corn.market.account.domain;

public class AccountDto {

	private String user_id;
	private String user_name;
	private String phone;
	private String email;

	public AccountDto() {}

	public AccountDto(String user_id, String user_name, String phone, String email) {
		this.user_id = user_id;
		this.user_name = user_name;
		this.phone = phone;
		this.email = email;
	}

	@Override
	public String toString() {
		return "AccountDto{" +
				"user_id='" + user_id + '\'' +
				", user_name='" + user_name + '\'' +
				", phone='" + phone + '\'' +
				", email='" + email + '\'' +
				'}';
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

}