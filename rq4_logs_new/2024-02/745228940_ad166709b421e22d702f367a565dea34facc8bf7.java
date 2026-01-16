package org.example.JavaTaigaCode.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.example.JavaTaigaCode.models.UserStoryDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class TaskServiceTest {

    @Test
    public void testGetClosedStories() throws Exception {
        // Arrange
        Tasks tasks = new Tasks();
        ObjectMapper objectMapper = new ObjectMapper();
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = new StringEntity("[{\"is_closed\":true,\"created_date\":\"2023-01-01\",\"finish_date\":\"2023-01-10\"},{\"is_closed\":false}]");
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
//        tasks.httpClient = httpClient;

        // Act
        List<UserStoryDTO> result = tasks.getClosedStories(123);

        // Assert
        assertEquals(null, result);

    }

}