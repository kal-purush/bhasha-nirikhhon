package com.project.Link.Dbinfo;

public class DBinfo {
	private static final String driver = "com.mysql.cj.jdbc.Driver";
 	private static final String url = "jdbc:mysql://localhost:3306/Link?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
 	private static final String userId = "root";
 	private static final String userPw = "root";
 	
 	
	public static String getDriver() {
		return driver;
	}
	public static String getUrl() {
		return url;
	}
	public static String getUserid() {
		return userId;
	}
	public static String getUserpw() {
		return userPw;
	}
}