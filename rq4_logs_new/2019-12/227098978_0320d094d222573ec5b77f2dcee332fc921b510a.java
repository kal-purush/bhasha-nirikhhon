package com.zhangwei.cms.pojo;

import java.util.Date;

public class User {
	private Integer id           ;
	private String username     ;
	private String password     ;
	private String nickname     ;
	private Date birthday     ;
	private Integer gender       ;
	private Integer locked       ;
	private Date creat_time   ;
	private Date update_time  ;
	private String url          ;
	private Integer score        ;
	private String role         ;
	@Override
	public String toString() {
		return "User [id=" + id + ", username=" + username + ", password=" + password + ", nickname=" + nickname
				+ ", birthday=" + birthday + ", gender=" + gender + ", locked=" + locked + ", creat_time=" + creat_time
				+ ", update_time=" + update_time + ", url=" + url + ", score=" + score + ", role=" + role + "]";
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
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
	public String getNickname() {
		return nickname;
	}
	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	public Date getBirthday() {
		return birthday;
	}
	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}
	public Integer getGender() {
		return gender;
	}
	public void setGender(Integer gender) {
		this.gender = gender;
	}
	public Integer getLocked() {
		return locked;
	}
	public void setLocked(Integer locked) {
		this.locked = locked;
	}
	public Date getCreat_time() {
		return creat_time;
	}
	public void setCreat_time(Date creat_time) {
		this.creat_time = creat_time;
	}
	public Date getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public Integer getScore() {
		return score;
	}
	public void setScore(Integer score) {
		this.score = score;
	}
	public String getRole() {
		return role;
	}
	public void setRole(String role) {
		this.role = role;
	}
	public User(Integer id, String username, String password, String nickname, Date birthday, Integer gender,
			Integer locked, Date creat_time, Date update_time, String url, Integer score, String role) {
		super();
		this.id = id;
		this.username = username;
		this.password = password;
		this.nickname = nickname;
		this.birthday = birthday;
		this.gender = gender;
		this.locked = locked;
		this.creat_time = creat_time;
		this.update_time = update_time;
		this.url = url;
		this.score = score;
		this.role = role;
	}
	public User() {
		super();
		// TODO Auto-generated constructor stub
	}

	
	
	
}