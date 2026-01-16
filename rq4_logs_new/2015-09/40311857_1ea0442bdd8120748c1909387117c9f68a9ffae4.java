package com.aidar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:WEB-INF/dispatcher-servlet.xml"})
@WebAppConfiguration
public class RestControllerTest {

    private MockMvc mockMvc;

    @Autowired
    private RestController restController;
    @Autowired
    private UrlCollection urls;

    @Autowired
    WebApplicationContext webApplicationContext;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void showHomePageTest() throws Exception {
        mockMvc.perform(get("/home"))
                .andExpect(status().isOk())
                .andExpect(view().name("page"))
                .andExpect(model().attribute("url", urls.getAuthorizeUrl()));
    }

    //Эти методы не работают корректно. Потому что реальных данных для запросов нет и они приводят к ошибке.
    @Test
    public void loginTest() throws Exception {
        Token token = new Token("some accessToken", 100, 101);
        when(restController.login((HttpServletRequest) anyObject())).thenReturn(token);
        String code = "someCode";
        mockMvc.perform(get("/login?code=" + code))
                .andExpect(status().isOk())
                .andExpect(request().attribute("code", code))
                .andExpect(content().contentType(new MediaType(MediaType.APPLICATION_JSON.getType(),
                        MediaType.APPLICATION_JSON.getSubtype(),
                        Charset.forName("utf8"))))
                .andExpect(jsonPath("$.access_token", is("some accessToken")))
                .andExpect(jsonPath("$.expires_in", is(100)))
                .andExpect(jsonPath("$.user_id", is(101)));
    }

    @Test
    public void audiosTest() throws Exception {
        AudioCollectionResponse audioCollectionResponse = new AudioCollectionResponse();
        List<Audio> audiosList = new LinkedList<>();
        audiosList.add(new Audio(0, 1, "Aidar", "BestSong"));
        audioCollectionResponse.setResponse(new AudioCollection(1, audiosList));
        when(restController.audios((HttpServletRequest) anyObject())).thenReturn(audioCollectionResponse);
        mockMvc.perform(get("/audios"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(new MediaType(MediaType.APPLICATION_JSON.getType(),
                        MediaType.APPLICATION_JSON.getSubtype(),
                        Charset.forName("utf8"))))
                .andExpect(jsonPath("$.response.count", is(1)))
                .andExpect(jsonPath("$.response.items[0].id", is(0)))
                .andExpect(jsonPath("$.response.items[0].owner_id", is(1)))
                .andExpect(jsonPath("$.response.items[0].artist", is("Aidar")))
                .andExpect(jsonPath("$.response.items[0].title", is("BestSong")));
    }

    @Test
    public void deleteTest() throws Exception {
        String res = "1";
        when(restController.delete((HttpServletRequest) anyObject())).thenReturn(res);
        mockMvc.perform(get("/delete"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(new MediaType(MediaType.APPLICATION_JSON.getType(),
                        MediaType.APPLICATION_JSON.getSubtype(),
                        Charset.forName("utf8"))))
                .andExpect(jsonPath("$.response", is(res)));
    }

}