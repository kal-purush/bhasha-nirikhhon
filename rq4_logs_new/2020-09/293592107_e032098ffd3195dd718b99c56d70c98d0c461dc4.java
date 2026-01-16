package com.example.helloworld.domain;

public class Message {
	private String name;
	private String greeting;

	public Message(String name, String greeting) {
		this.name = name;
		this.greeting = greeting;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGreeting() {
		return greeting;
	}

	public void setGreeting(String greeting) {
		this.greeting = greeting;
	}
}