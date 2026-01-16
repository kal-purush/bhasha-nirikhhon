package com.aaron.kata.babysitter;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest
public class HoursCalculationControllerTest {

    @Autowired
    private MockMvc mvc;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testHomePageIsDisplayedWithFormBoxes() throws Exception {
        mvc.perform(get("/"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Start Time:")))
                .andExpect(content().string(containsString("End Time:")));
    }

    @Test
    public void testSuccessfulInputSubmissionRedirectsToResults() throws Exception {
        MvcResult result = mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "5:30PM")
                .param("endTime", "3:30AM")).andReturn();

        assertEquals("redirect:/results",result.getModelAndView().getViewName());
        content().string(containsString("Salary Earned:"));
    }

    @Test
    public void testNullInputErrorMessage() throws Exception {
       mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "")
                .param("endTime", "")
       ).andExpect(content().string(containsString("Please enter a valid time in AM or PM")));
    }

    @Test
    public void testNonTimeUnitInputErrorMessage() throws Exception {
        mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "5")
                .param("endTime", "3")
        ).andExpect(content().string(containsString("Invalid time format, please enter hours and minutes." +
                " Example: 4:30pm")));
    }

    @Test
    public void testMilitaryTimeInputErrorMessage() throws Exception {
        mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "23:00")
                .param("endTime", "15:00")
        ).andExpect(content().string(containsString("Invalid time format, please enter hours and minutes." +
                " Example: 4:30pm")));
    }

    @Test
    public void testMalformedHoursInputErrorMessage() throws Exception {
        mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "3:00 PM")
                .param("endTime", "1:00 AM")
        ).andExpect(content().string(containsString("Invalid time format, please enter hours and minutes." +
                " Example: 4:30pm")));
    }

    @Test
    public void testStartTimeFailsBefore5PM() throws Exception {
        MvcResult result = mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "12:30PM")
                .param("endTime", "3:30PM")).andReturn();

        assertEquals("redirect:/error",result.getModelAndView().getViewName());
        content().string(containsString("Start time entered was too early"));
    }

    @Test
    public void testEndTimeFailsAfter4AM() throws Exception {
        MvcResult result = mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("startTime", "12:30PM")
                .param("endTime", "3:30PM")).andReturn();

        assertEquals("redirect:/error",result.getModelAndView().getViewName());
        content().string(containsString("End time entered was too late"));
    }
}