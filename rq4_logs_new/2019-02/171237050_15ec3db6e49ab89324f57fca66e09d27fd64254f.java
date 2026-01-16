package com.nt.command;

import java.util.Date;

public class UserCommand {
private String username,pwd;
private String dmn;
private Date date;
public String getUsername() {
	return username;
}
public void setUsername(String username) {
	this.username = username;
}
public String getPwd() {
	return pwd;
}
public void setPwd(String pwd) {
	this.pwd = pwd;
}
public String getDmn() {
	return dmn;
}
public void setDmn(String dmn) {
	this.dmn = dmn;
}
public Date getDate() {
	return date;
}
public void setDate(Date date) {
	this.date = date;
}
}