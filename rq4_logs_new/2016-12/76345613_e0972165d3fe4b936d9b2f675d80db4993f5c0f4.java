package com.momo.struts.action;

import com.momo.struts.exceptions.RegisterException;
import com.momo.struts.util.StringTool;
import com.opensymphony.xwork2.ActionSupport;

@SuppressWarnings("serial")
public class RegisterAction extends ActionSupport {
	
	private String username;
	private String password;
	private String pwdretype;
	
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
	public String getPwdretype() {
		return pwdretype;
	}
	public void setPwdretype(String pwdretype) {
		this.pwdretype = pwdretype;
	}
	
	public String execute() {
		if(StringTool.isEmpty(username)) throw new RegisterException("请输入用户名");
		if(StringTool.isEmpty(password)) throw new RegisterException("请输入密码");
		if(StringTool.isEmpty(pwdretype)) throw new RegisterException("请再次输入密码");
		if(username.length() > 16 || username.length() < 8) 
			throw new RegisterException("用户名长度必须为8~16位字母、数字、符号");
		if(!password.equals(pwdretype))
			throw new RegisterException("两次密码不一致");
		return SUCCESS;
	}

}