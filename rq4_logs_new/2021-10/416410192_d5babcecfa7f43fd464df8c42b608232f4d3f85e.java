package com.capgemeni.railwayAuthentication.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.capgemeni.railwayAuthentication.Entity.Userinfo;
import com.capgemeni.railwayAuthentication.Repository.UserInfoRepository;

@RunWith(SpringRunner.class)
@SpringBootTest({ "server.port:0", "eureka.client.enabled:false" })
class AuthenticationControllerTest {

	@Autowired
	private AuthenticationController authController;

	@MockBean
	UserInfoRepository userinfoRepo;

	@Test
	void testGetAllUsers() {
		List<Userinfo> list=new ArrayList<>();
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		list.add(userinfo);
		Mockito.when(userinfoRepo.findAll()).thenReturn(list);
		List<Userinfo> users = authController.getAllUsers();
		System.out.println("USers size" + users.size());
		assertNotNull(users);
		assertEquals(users.size(), 1);
	}

	@Test
	void testAddUser_success() throws Exception {
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		Mockito.when(userinfoRepo.save(userinfo)).thenReturn(userinfo);
		String result=authController.addUser(userinfo);
		assertEquals("success", result);
		
	}
	
	@Test
	void testAddUser_failure() throws Exception {
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		Mockito.when(userinfoRepo.save(userinfo)).thenReturn(null);
		String result=authController.addUser(userinfo);
		assertEquals("Failed Signup", result);
	}

	@Test
	void testLogin_success() {
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		List<Userinfo> usersList=new ArrayList<>();
		usersList.add(userinfo);
		Mockito.when(userinfoRepo.findByusername("bhanu")).thenReturn(usersList);
		Userinfo result=authController.login("bhanu", "manchuri");
		assertNotNull(result);
	}
	
	@Test
	void testLogin_failure() {
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		List<Userinfo> usersList=new ArrayList<>();
		Mockito.when(userinfoRepo.findByusername("bhanu")).thenReturn(usersList);
		Userinfo result=authController.login("bhanu", "manchuri");
		assertNull(result);
	}

	@Test
	void testGetUserinfoByUsername() {
		Userinfo userinfo = new Userinfo("bhanu", "manchuri", "9581050964", "madanapalli", "User");
		List<Userinfo> usersList=new ArrayList<>();
		usersList.add(userinfo);
		Mockito.when(userinfoRepo.findByusername("bhanu")).thenReturn(usersList);
		Userinfo list=authController.getUserinfoByUsername("bhanu");
		assertNotNull(list);
		assertEquals(list.getUsername(), "bhanu");
	}

}