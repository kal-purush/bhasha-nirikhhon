package com.matrix.ssm.pojo;

public class Person {
	private String name ;
    private Integer age ;
	private String gender;
	private String insCode;
    public Person(String name, Integer age, String gender) {
		super();
		this.name = name;
		this.age = age;
		this.gender = gender;
	}


	public Person(String name, Integer age, String gender, String insCode) {
		super();
		this.name = name;
		this.age = age;
		this.gender = gender;
		this.insCode = insCode;
	}


	public String getInsCode() {
		return insCode;
	}


	public void setInsCode(String insCode) {
		this.insCode = insCode;
	}


	public String getGender() {
		return gender;
	}


	public void setGender(String gender) {
		this.gender = gender;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}



    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }
}