package com.smoothstack.lms.adminmicroservice;

import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;

import com.smoothstack.lms.adminmicroservice.controller.AdminController;
import com.smoothstack.lms.adminmicroservice.model.Author;
import com.smoothstack.lms.adminmicroservice.model.Book;
import com.smoothstack.lms.adminmicroservice.model.Borrower;
import com.smoothstack.lms.adminmicroservice.model.Branch;
import com.smoothstack.lms.adminmicroservice.model.Copies;
import com.smoothstack.lms.adminmicroservice.model.Genre;
import com.smoothstack.lms.adminmicroservice.model.Publisher;

@SpringBootTest
class AdminConrollerTest {

	@Autowired
	AdminController adminController;

	@Test
	void testAuthor() throws ClassNotFoundException, SQLException {
		Author author = new Author();
		author.setAuthorName("Test");
		author.setBooks(new ArrayList<Book>());
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveAuthor(author).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getAuthor(author.getAuthorId()).getStatusCode());
		author.setAuthorName("Test2");
		Assertions.assertEquals(HttpStatus.OK,
				adminController.updateAuthor(author, author.getAuthorId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT,
				adminController.deleteAuthor(author.getAuthorId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getAuthor(author.getAuthorId()).getStatusCode());
	}

	@Test
	void testReadAuthor() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getAuthors().getStatusCode());
	}

	@Test
	void testBook() throws ClassNotFoundException, SQLException {
		Book book = new Book();
		book.setAuthors(new ArrayList<Author>());
		Author author = new Author();
		author.setAuthorId(1);
		book.getAuthors().add(author);
		book.setGenres(new ArrayList<Genre>());
		book.setPublisher(new Publisher());
		book.getPublisher().setPublisherId(1);
		book.setTitle("Test");
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveBook(book).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getBook(book.getBookId()).getStatusCode());
		book.setTitle("Test2");
		Assertions.assertEquals(HttpStatus.OK, adminController.updateBook(book, book.getBookId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT, adminController.deleteBook(book.getBookId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getBook(book.getBookId()).getStatusCode());
	}

	@Test
	void testReadBook() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getBooks().getStatusCode());
	}

	@Test
	void testGenre() throws ClassNotFoundException, SQLException {
		Genre genre = new Genre();
		genre.setGenreName("Test");
		genre.setBooks(new ArrayList<Book>());
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveGenre(genre).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getGenre(genre.getGenreId()).getStatusCode());
		genre.setGenreName("Test2");
		Assertions.assertEquals(HttpStatus.OK, adminController.updateGenre(genre, genre.getGenreId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT, adminController.deleteGenre(genre.getGenreId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getGenre(genre.getGenreId()).getStatusCode());
	}

	@Test
	void testReadGenre() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getGenres().getStatusCode());
	}
	
	@Test
	void testBorrower() throws ClassNotFoundException, SQLException {
		Borrower borrower = new Borrower();
		borrower.setName("Test");
		borrower.setAddress("123");
		borrower.setPhone("123");
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveBorrower(borrower).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getBorrower(borrower.getCardNo()).getStatusCode());
		borrower.setName("Test2");
		Assertions.assertEquals(HttpStatus.OK,
				adminController.updateBorrower(borrower, borrower.getCardNo()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT,
				adminController.deleteBorrower(borrower.getCardNo()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getBorrower(borrower.getCardNo()).getStatusCode());
	}

	@Test
	void testReadBorrower() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getBorrowers().getStatusCode());
	}
	
	@Test
	void testBranch() throws ClassNotFoundException, SQLException {
		Branch branch = new Branch();
		branch.setBranchName("Test");
		branch.setBranchAddress("123");
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveBranch(branch).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getBranch(branch.getBranchId()).getStatusCode());
		branch.setBranchName("Test2");
		Assertions.assertEquals(HttpStatus.OK,
				adminController.updateBranch(branch, branch.getBranchId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT,
				adminController.deleteBranch(branch.getBranchId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getBranch(branch.getBranchId()).getStatusCode());
	}

	@Test
	void testReadBranch() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getBranchs().getStatusCode());
	}
	
	@Test
	void testCopies() throws ClassNotFoundException, SQLException {
		Copies copy = new Copies();
		copy.setBookId(5);
		copy.setBranchId(3);
		copy.setNoOfCopies(66);
		Assertions.assertEquals(HttpStatus.CREATED, adminController.saveCopies(copy).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getCopy(copy.getBranchId(), copy.getBookId()).getStatusCode());
		copy.setNoOfCopies(67);
		Assertions.assertEquals(HttpStatus.OK,
				adminController.updateCopies(copy, copy.getBranchId(), copy.getBookId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT,
				adminController.deleteCopies(copy.getBranchId(), copy.getBookId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getCopy(copy.getBranchId(), copy.getBookId()).getStatusCode());
	}

	@Test
	void testReadCopies() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getCopies().getStatusCode());
	}
	
	@Test
	void testPublisher() throws ClassNotFoundException, SQLException {
		Publisher publisher = new Publisher();
		publisher.setPublisherName("Test");
		publisher.setPublisherAddress("123");
		publisher.setPublisherPhone("123");
		Assertions.assertEquals(HttpStatus.CREATED, adminController.savePublisher(publisher).getStatusCode());
		Assertions.assertEquals(HttpStatus.OK, adminController.getPublisher(publisher.getPublisherId()).getStatusCode());
		publisher.setPublisherName("Test2");
		Assertions.assertEquals(HttpStatus.OK,
				adminController.updatePublisher(publisher, publisher.getPublisherId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NO_CONTENT,
				adminController.deletePublisher(publisher.getPublisherId()).getStatusCode());
		Assertions.assertEquals(HttpStatus.NOT_FOUND, adminController.getPublisher(publisher.getPublisherId()).getStatusCode());
	}

	@Test
	void testReadPublisher() throws ClassNotFoundException, SQLException {
		Assertions.assertEquals(HttpStatus.OK, adminController.getPublishers().getStatusCode());
	}

}