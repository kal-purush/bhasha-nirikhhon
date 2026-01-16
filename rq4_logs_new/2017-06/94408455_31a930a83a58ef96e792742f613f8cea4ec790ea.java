package com.ac.common.fabric.config;

import lombok.Data;

@Data
public class OrgCommonConfig {

	private String id;

	private String name;
	
	private String mspid;

	private String domname;

	private String caLocation;

	private String peerLocations;

	private String ordererLocations;

	private String eventhubLocations;

	private Admin admin = new Admin();

	private User user = new User();

	@Data
	public static class Admin {
		private String name;
		private String password;
	}

	@Data
	public static class User {
		private String name;
		private String password;
	}

}