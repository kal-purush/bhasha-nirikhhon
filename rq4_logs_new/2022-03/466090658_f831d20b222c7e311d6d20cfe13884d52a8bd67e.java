package com.qa.dvdLibrary.controller;


import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultMatcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qa.dvdLibrary.entity.Dvd;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class DvdControllerIntegrationTest {

	@Autowired
	private MockMvc mvc;
	
	@Autowired
	private ObjectMapper mapper;
	
	@Test
	void createDvdTest() throws Exception {
		//Given
		Dvd newDvd = new Dvd("Stardust", "Romance", 2007, 122, "PG", "Claire Danes", "Robert De Niro");
		String newDvdJSON = this.mapper.writeValueAsString(newDvd);
		
		Dvd dvdSaved = new Dvd(1, "Stardust", "Romance", 2007, 122, "PG", "Claire Danes", "Robert De Niro");
		String dvdSavedJSON = this.mapper.writeValueAsString(dvdSaved);
		//when
		RequestBuilder request = post("/create")
				.contentType(MediaType.APPLICATION_JSON)
				.content(newDvdJSON);
		
		ResultMatcher responseStatus = status().isCreated();
		ResultMatcher responseContent = content().json(dvdSavedJSON);
		
		//then
		this.mvc.perform(request).andExpect(responseStatus).andExpect(responseContent);
	}
}