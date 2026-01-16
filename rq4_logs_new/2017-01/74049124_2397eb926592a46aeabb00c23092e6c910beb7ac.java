/**
 * 
 */
package com.aexample.test.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

import com.aexample.test.JUnitTestConfiguration;
import com.aexample.website.controller.DashboardController;


/**
 * @author Main Login
 * $Rev$
 * $Date$
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={JUnitTestConfiguration.class})
@WebAppConfiguration
public class DashboardControllerTest {
	private static final Logger logger = LoggerFactory.getLogger(DashboardController.class);
	
    @Autowired
    private WebApplicationContext ctx;
    
    private MockMvc mockMvc;
    
    @Configuration
    public static class TestConfiguration {
 
        @Bean public DashboardController dashboardController() {
            return new DashboardController();
        }
 
    } 
    
    @Before public void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(ctx).build();
        InternalResourceViewResolver viewResolver = new InternalResourceViewResolver();
        viewResolver.setPrefix("/WEB-INF/pages/");
        viewResolver.setSuffix(".jsp");
 
        mockMvc = MockMvcBuilders.standaloneSetup(new DashboardController())
                                 .setViewResolvers(viewResolver)
                                 .build();
    }
	
    @Test
    public void testDashboard() throws Exception {

        mockMvc.perform(get("/dashboard"))
            .andExpect(status().isOk())
            .andExpect(forwardedUrl("/WEB-INF/pages/dashboard.jsp"))
            .andExpect(model().attributeExists("message"));
        logger.info("Passed testDashboard test");
    }
    
   
    
}