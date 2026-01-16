package com.javalearning.springboot.helloworld.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "spring.database")
//@Data
public class DatasourceProps {

	private String url;
	private String username;
	private String password;

	public DatasourceProps() {
	}

	public DatasourceProps(String url, String username, String password) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

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

	@Override
	public String toString() {
		return "DatasourceProps [url=" + url + ", username=" + username + ", password=" + password + "]";
	}

}