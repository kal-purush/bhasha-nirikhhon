package ch.uzh.ifi.hase.soprafs23.controller;

import ch.uzh.ifi.hase.soprafs23.constant.ApiUrls;
import ch.uzh.ifi.hase.soprafs23.rest.dto.TopicGetDTO;
import ch.uzh.ifi.hase.soprafs23.service.ApiService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc
public class CategoriesApiCallIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ApiService apiService;

    @Test
    public void testGetTopics() throws Exception {
        List<TopicGetDTO> topics = apiService.getTopicsFromApi(ApiUrls.CATEGORIES.url);

        mockMvc.perform(MockMvcRequestBuilders
                        .get("/categories")
                        .header("Authorization", "Bearer " + "top-secret-token")
                ).andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].topicName", equalTo(topics.get(0).getTopicName())));
    }
}