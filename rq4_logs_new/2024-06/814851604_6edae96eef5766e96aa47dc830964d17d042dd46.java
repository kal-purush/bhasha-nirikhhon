package com.bookshelf.domain;

import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import com.bookshelf.domain.enums.*;

@Entity
public class Book implements Serializable {
	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Integer id;
	protected String title;
	protected String author;
	
	@ElementCollection(fetch = FetchType.EAGER)
	@CollectionTable(name = "STATUS")
	protected Set<StatusReading> readingStatus = new HashSet<>(); // "Read", "Reading", "Want to Read"
	
	@ElementCollection(fetch = FetchType.EAGER)
	@CollectionTable(name = "FORMATS")
	protected Set<Format> format = new HashSet<>();  // e.g., Hardcover, Paperback, eBook, Audiobook
	
	public Book() {
		super();
	}
	
	public Book(Integer id, String title, String author) {
		super();
		this.id = id;
		this.title = title;
		this.author = author;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Set<StatusReading> getReadingStatus() {
		return readingStatus;
	}

	public void setReadingStatus(Set<StatusReading> readingStatus) {
		this.readingStatus = readingStatus;
	}

	public Set<Format> getFormat() {
		return format;
	}

	public void setFormat(Set<Format> format) {
		this.format = format;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Book other = (Book) obj;
		return Objects.equals(id, other.id);
	}
	
}