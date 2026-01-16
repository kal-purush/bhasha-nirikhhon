package com.springboot.basics;

public class Book {
	
	long id;
	String Name;
	String Author;
	
	
	public Book(long id, String name, String author) {
		super();
		this.id = id;
		Name = name;
		Author = author;
	}


	public long getId() {
		return id;
	}


	public String getName() {
		return Name;
	}


	public String getAuthor() {
		return Author;
	}


	@Override
	public String toString() {
		return "Book [id=" + id + ", Name=" + Name + ", Author=" + Author + "]";
	}


}