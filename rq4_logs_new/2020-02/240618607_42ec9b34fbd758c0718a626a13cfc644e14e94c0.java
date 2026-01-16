package com.smoothstack.lms.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.smoothstack.lms.model.BookCopy;

@Component
public class BookCopyDao {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private BookDao bookDao;

	@Autowired
	private LibraryBranchDao libraryBranchDao;

	public void update(BookCopy bookCopy) throws SQLException {
		jdbcTemplate.update("update tbl_book_copies set noOfCopies = ? where bookId = ? and branchId = ?",
				new Object[] { bookCopy.getAmount(), bookCopy.getBook().getId(), bookCopy.getLibraryBranch().getId() });
	}

	public BookCopy readOne(int bookId, int libraryBranchId) throws SQLException {
		return jdbcTemplate.queryForObject("select * from tbl_book_copies where bookId = ? and branchId = ?",
				new Object[] { bookId, libraryBranchId }, (rs, rowNum) -> extractData(rs));
	}

	public BookCopy extractData(ResultSet rs) throws SQLException {
		BookCopy bookCopy = new BookCopy();
		bookCopy.setBook(bookDao.readOneFirstLevel(rs.getInt("bookId")));
		bookCopy.setLibraryBranch(libraryBranchDao.readOneFirstLevel(rs.getInt("branchId")));
		bookCopy.setAmount(rs.getInt("noOfCopies"));
		return bookCopy;
	}

}