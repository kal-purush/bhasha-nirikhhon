package com.network;

import lombok.Data;

@Data
public class ProxyLogin {
	private String user;
	private String password;
	public ProxyLogin(String user, String password) {
		setUser(user);
		setPassword(password);
	}

}