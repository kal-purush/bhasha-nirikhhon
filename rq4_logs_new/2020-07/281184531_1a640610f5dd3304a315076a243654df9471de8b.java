package com.smbcgroup.training.atm.api;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.smbcgroup.training.atm.ATMService;
import com.smbcgroup.training.atm.User;
import com.smbcgroup.training.atm.dao.UserNotFoundException;

public class APIControllerTest {

	private APIController apiController = new APIController();
	private MockMvc mockMvc;

	@Before
	public void setup() {
		mockMvc = MockMvcBuilders.standaloneSetup(apiController).build();
	}

	@Test
	public void getUser_NotFound() throws Exception {
		APIController.service = new ATMService(null) {
			@Override
			public User getUser(String userId) throws UserNotFoundException {
				throw new UserNotFoundException();
			}
		};
		MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/users/schan")).andReturn();
		assertEquals(404, result.getResponse().getStatus());
	}

	@Test
	public void getUser_Found() throws Exception {
		APIController.service = new ATMService(null) {
			@Override
			public User getUser(String userId) throws UserNotFoundException {
				User user = new User();
				user.setUserId("rdelaney");
				user.setAccounts(new String[] { "123456" });
				return user;
			}
		};
		MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/users/rdelaney")).andReturn();
		assertEquals(200, result.getResponse().getStatus());
		assertEquals("{\"userId\":\"rdelaney\",\"accounts\":[\"123456\"]}", result.getResponse().getContentAsString());
	}

}