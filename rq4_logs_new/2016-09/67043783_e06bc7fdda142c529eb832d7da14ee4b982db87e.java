package com.example;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import springfox.documentation.staticdocs.Swagger2MarkupResultHandler;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by kostya.nikitin on 9/9/2016.
 */
@RunWith(SpringRunner.class)
@PropertySource("classpath:swagger.properties")
@SpringBootTest
public class SwaggerApiDefenitionTest {

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext context;

    @Before
    public void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
                .alwaysDo(print())
                .build();
    }

    @Value("${springfox.documentation.swagger.v2.path}")
    private String swagger2Endpoint;

    @Test
    public void convertSwaggerToAsciiDoc() throws Exception {
        String outputDir = System.getProperty("io.springfox.staticdocs.outputDir");

        this.mockMvc.perform(get(swagger2Endpoint)

                .accept(MediaType.APPLICATION_JSON))
                .andDo(Swagger2MarkupResultHandler.outputDirectory(outputDir).build())
                .andExpect(status().isOk());
    }


}