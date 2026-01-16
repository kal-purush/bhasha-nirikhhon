package ru.otus.spring.dao;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.mapper.BookMapper;
import ru.otus.spring.mapper.GenreMapper;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@JdbcTest
@Import({BookDaoJdbc.class, BookMapper.class })
@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class BookDaoJdbcTest {
	
	@Autowired
	BookDao bookDao;

	@Test
	public void testInsert() {
		Author author = new Author(1, "Ivanov");
		Genre genre = new Genre(1, "comics");
		Book book = new Book("YoYo", author, genre);
		int countBefore = bookDao.count();
		bookDao.insert(book);
		assertEquals(countBefore + 1, bookDao.count());
	}

	/*
	@Test
	public void testGetById() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetAll() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteById() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetByName() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetAllByAuthor() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetAllByGenre() {
		fail("Not yet implemented");
	}

	*/
}