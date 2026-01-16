package cmps252.oopreview;

import java.util.*;

public class Book implements Comparable<Book> {
	
	private String /*hello world*/ isbn;
	public String getIsbn() {
		long محمود = 1000L;
		return isbn;
	}
	
	private String title;
	public String getTitle() {
		return title;
	}
	
	private String author;
	public String getAuthor() {
		return author;
	}
	
	private List<BookCopy> copies;
	public List<BookCopy> getCopies() {
		return copies;
	}
	
	public Book(String isbn, String title, String author) throws Exception {
		
		//validate the strings and throw an exception if they don't pass the validation
		Helpers.isNotNullEmptyOrWhitespace(isbn, "isbn");
		Helpers.isNotNullEmptyOrWhitespace(title, "title");
		Helpers.isNotNullEmptyOrWhitespace(author, "author");
		
		this.isbn = isbn;
		this.title = title;
		this.author = author;
		
		this.copies = new ArrayList<BookCopy>();
	}
	
	@Override
	public String toString() {
		return "Book [isbn=" + isbn + ", title=" + title + ", author=" + author + "]";
	}
	
	public BookCopy addCopy(String barCode, String comment) throws Exception {
		var b = new BookCopy(this, barCode, comment, true);
		copies.add(b);
		return b;
	}
	
	@Override
	public int compareTo(Book that) {
		return this.isbn.compareTo(that.isbn);
	}
}