package memberDto;

import java.io.Serializable;
/*
DROP TABLE USER_INFO
CASCADE CONSTRAINTS;

CREATE TABLE USER_INFO(
	ID VARCHAR2(50) PRIMARY KEY,
	PWD VARCHAR2(50) NOT NULL,
	NAME VARCHAR2(50) NOT NULL,
	EMAIL VARCHAR2(50) UNIQUE,
	PHONE VARCHAR2(50) NOT NULL,
	ADDRESS VARCHAR2(100) NOT NULL,
	AUTH NUMBER(1) NOT NULL
);
*/
public class memberDto implements Serializable {
	
	private String id;
	private String pwd;
	private String name;
	private String email;
	private String phone;
	private String address;
	private int auth;
	
	public memberDto() {
		super();
		// TODO Auto-generated constructor stub
	}

	public memberDto(String id, String pwd, String name, String email, String phone, String address, int auth) {
		this.id = id;
		this.pwd = pwd;
		this.name = name;
		this.email = email;
		this.phone = phone;
		this.address = address;
		this.auth = auth;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getAuth() {
		return auth;
	}

	public void setAuth(int auth) {
		this.auth = auth;
	}

	@Override
	public String toString() {
		return "memberDto [id=" + id + ", pwd=" + pwd + ", name=" + name + ", email=" + email + ", phone=" + phone
				+ ", address=" + address + ", auth=" + auth + "]";
	}
}