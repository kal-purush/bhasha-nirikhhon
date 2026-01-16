package com.example.model;


import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest(classes = User.class)
class MemoTests {

	private int testUserUserId = 123;
	private String testUserUsername = "JT";
	private String testUserPassword = "password1";
	private String testUserRole = "Buyer";
	private String testUserEmail = "jt@test.com";
	private User testUser = new User(testUserUserId, testUserUsername, testUserPassword, testUserRole, testUserEmail);
	private int testUserUserIdToUpdate = 321;
	private String testUserUsernameToUpdate = "JT123";
	private String testUserPasswordToUpdate = "password2";
	private String testUserRoleToUpdate = "Seller";
	private String testUserEmailToUpdate = "jt123@test.com";
	@Test
	void testSetGetId() {	
		assertEquals(testUserUserId, testUser.getUserId());
		testUser.setUserId(testUserUserIdToUpdate);
		assertEquals(testUserUserIdToUpdate, testUser.getUserId());
	}
	
	@Test
	void testSetGetUsername() {
		assertEquals(testUserUsername, testUser.getUsername());
		testUser.setUsername(testUserUsernameToUpdate);
		assertEquals(testUserUsernameToUpdate, testUser.getUsername());
	}
	
	@Test
	void testSetGetPassword() {
		assertEquals(testUserPassword, testUser.getPassword());
		testUser.setPassword(testUserPasswordToUpdate);
		assertEquals(testUserPasswordToUpdate, testUser.getPassword());
	}
	
	void testSetGetRole() {
		assertEquals(testUserRole, testUser.getRole());
		testUser.setRole(testUserRoleToUpdate);
		assertEquals(testUserRoleToUpdate, testUser.getRole());
	}
	
	void testSetGetEmail() {
		assertEquals(testUserEmail, testUser.getEmail());
		testUser.setEmail(testUserEmailToUpdate);
		assertEquals(testUserEmailToUpdate, testUser.getEmail());
	}

}