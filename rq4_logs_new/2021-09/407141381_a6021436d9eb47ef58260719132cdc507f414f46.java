package com.lucamusic.event;
//package com.lucamusic.user;
//
//import static org.mockito.Mockito.when;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
//import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
//
//import java.time.LocalDate;
//import java.util.Date;
//
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
//import org.springframework.boot.test.mock.mockito.MockBean;
//import org.springframework.test.context.junit.jupiter.SpringExtension;
//import org.springframework.test.web.servlet.MockMvc;
//
//import com.lucamusic.user.controller.UserController;
//import com.lucamusic.user.entity.User;
//import com.lucamusic.user.service.UserServiceImpl;
//

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import com.lucamusic.event.controller.EventController;
import com.lucamusic.event.entity.Event;
import com.lucamusic.event.service.EventServiceImpl;

@WebMvcTest(EventController.class)
public class TestGetEventFiltered {

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private EventServiceImpl serv;
	@Test
	void assertUserIsCreated() throws Exception {


		List<Event> eventList = List.of(new Event());

		when(serv.findByMusicStyle("Test")).thenReturn(eventList);

		mockMvc
		.perform(get("/events").param("musicStyle", "Test"))
		.andDo(print())
		.andExpect(status().isOk());
	}
}