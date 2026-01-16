package com.cst438;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import java.sql.Date;
import java.util.Optional;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.cst438.controllers.AssignmentController;
import com.cst438.domain.Assignment;
import com.cst438.domain.AssignmentListDTO;
import com.cst438.domain.AssignmentRepository;
import com.cst438.domain.Course;
import com.cst438.domain.CourseRepository;
import com.fasterxml.jackson.databind.ObjectMapper;


@ContextConfiguration(classes = { AssignmentController.class })
@AutoConfigureMockMvc(addFilters = false)
@WebMvcTest
public class JunitTestAssignment {
	static final String URL = "http://localhost:8081";
	public static final int TEST_ASSIGNMENT_ID = 5;
	public static final String TEST_ASSIGNMENT_NAME = "Assignment 2b";
	public static final String TEST_DUE_DATE = "2023-03-01";
	public static final int COURSE_ID = 1234;

	@MockBean
	AssignmentRepository aRep;
	
	@MockBean
	CourseRepository cRep;

	@Autowired
	private MockMvc mvc;
	

	@Test
	public void createAssignment() throws Exception {
		
		MockHttpServletResponse response;
		
		// set up a test course
		Course c = new Course();
		c.setCourse_id(COURSE_ID);
		
		// define fake assignment object
		Assignment a = new Assignment();
		a.setId(TEST_ASSIGNMENT_ID);
		
		given(cRep.findById(COURSE_ID)).willReturn(Optional.of(c)); // returns a course object
		given(aRep.save(any())).willReturn(a); // can pass in anything from the assignment repo
		
		// pass in an assignment DTO
		// we don't really need a course title since the controller doesn't do anything with it
		AssignmentListDTO.AssignmentDTO as = new AssignmentListDTO.AssignmentDTO(0, COURSE_ID, TEST_ASSIGNMENT_NAME, TEST_DUE_DATE, null);
		// example of mvc perform
		// send updates to server
		response = mvc
				.perform(MockMvcRequestBuilders.post("/assignment").accept(MediaType.APPLICATION_JSON)
						.content(asJsonString(as)).contentType(MediaType.APPLICATION_JSON))
				.andReturn().getResponse();

		// check the result with the assertequals methods
		// verify that return status = OK (value 200)
		assertEquals(200, response.getStatus());
		
		// simulated call to the controller and it returned and assignment DTO
		AssignmentListDTO.AssignmentDTO result = fromJsonString(response.getContentAsString(), AssignmentListDTO.AssignmentDTO.class);
        assertNotEquals(0, result.assignmentId); 
	}
	@Test
	public void deleteAssignment() throws Exception {
		
		MockHttpServletResponse response;
		
		// set up a test course
		Course c = new Course();
		c.setCourse_id(COURSE_ID);
		
		// define fake assignment object
		Assignment a = new Assignment();
		a.setId(TEST_ASSIGNMENT_ID);
		
		given(cRep.findById(COURSE_ID)).willReturn(Optional.of(c)); // returns a course object
		given(aRep.save(any())).willReturn(a); // can pass in anything from the assignment repo
		
		// pass in an assignment DTO
		// we don't really need a course title since the controller doesn't do anything with it
		AssignmentListDTO.AssignmentDTO as = new AssignmentListDTO.AssignmentDTO(0, COURSE_ID, TEST_ASSIGNMENT_NAME, TEST_DUE_DATE, null);
		// example of mvc perform
		// send updates to server
		response = mvc
				.perform(MockMvcRequestBuilders.post("/assignment").accept(MediaType.APPLICATION_JSON)
						.content(asJsonString(as)).contentType(MediaType.APPLICATION_JSON))
				.andReturn().getResponse();

		// check the result with the assertequals methods
		// verify that return status = OK (value 200)
		assertEquals(200, response.getStatus());
		
		// simulated call to the controller and it returned and assignment DTO
		AssignmentListDTO.AssignmentDTO result = fromJsonString(response.getContentAsString(), AssignmentListDTO.AssignmentDTO.class);
        assertNotEquals(0, result.assignmentId); 
	}
	@Test
	public void updateAssignmentName() throws Exception {
		
		MockHttpServletResponse response;
		
		// set up a test course
		Course c = new Course();
		c.setCourse_id(COURSE_ID);
		
		// define fake assignment object
		Assignment a = new Assignment();
		a.setId(TEST_ASSIGNMENT_ID);
		
		given(cRep.findById(COURSE_ID)).willReturn(Optional.of(c)); // returns a course object
		given(aRep.save(any())).willReturn(a); // can pass in anything from the assignment repo
		
		// pass in an assignment DTO
		// we don't really need a course title since the controller doesn't do anything with it
		AssignmentListDTO.AssignmentDTO as = new AssignmentListDTO.AssignmentDTO(0, COURSE_ID, TEST_ASSIGNMENT_NAME, TEST_DUE_DATE, null);
		// example of mvc perform
		// send updates to server
		response = mvc
				.perform(MockMvcRequestBuilders.post("/assignment").accept(MediaType.APPLICATION_JSON)
						.content(asJsonString(as)).contentType(MediaType.APPLICATION_JSON))
				.andReturn().getResponse();

		// check the result with the assertequals methods
		// verify that return status = OK (value 200)
		assertEquals(200, response.getStatus());
		
		// simulated call to the controller and it returned and assignment DTO
		AssignmentListDTO.AssignmentDTO result = fromJsonString(response.getContentAsString(), AssignmentListDTO.AssignmentDTO.class);
        assertNotEquals(0, result.assignmentId); 
	}
	
	private static String asJsonString(final Object obj) {
		try {

			return new ObjectMapper().writeValueAsString(obj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static <T> T fromJsonString(String str, Class<T> valueType) {
		try {
			return new ObjectMapper().readValue(str, valueType);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}