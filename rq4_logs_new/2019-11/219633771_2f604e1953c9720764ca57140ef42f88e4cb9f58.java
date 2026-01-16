package com.test.dao;

import static org.junit.Assert.*;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.Test;

import com.project.dao.UsersDao;
import com.project.pojo.Posts;
import com.project.pojo.Users;

public class UserDaoTest {
	UsersDao ud = new UsersDao();
	Users testUser = new Users("Biggs", "Wedge", "email", "description", "profile", Date.valueOf(LocalDate.now()));
	Posts p = new Posts(LocalDateTime.now(), "Working on testing things", "Working on testing things");
	//USERS
	@Test
	public void testUserDaoExistance() {
		assertNotNull(ud);
	}
	@Test
	public void testUsernameInfo() {
		ud.InsertUserInfo(testUser);
		assertEquals("Voltsung", ud.getUserByUsername("Voltsung").getUsername());
	}
	@Test
	public void testUsersList() {
		assertNotNull(ud.getAll());
	}
	
	//POSTS
	@Test
	public void testPosts() {
		ud.InserPosts(p);
		assertEquals("Working on testing things", ud.getPostsByUsername(p));
		//I'm confused. This doesn't get the posts by the username. It gets posts by posts.
	}
	@Test
	public void testMainFeed() {
		assertNotNull(ud.getAllPosts());
	}
	
	//LIKES AND COMMENTS CANNOT BE TESTED YET
}