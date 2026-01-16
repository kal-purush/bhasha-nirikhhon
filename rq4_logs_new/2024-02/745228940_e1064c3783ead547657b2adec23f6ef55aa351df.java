package org.example.JavaTaigaCode.service;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.example.JavaTaigaCode.models.TaskDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.net.HttpURLConnection;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class CycleTimeServiceTest {
    @Test
    public void testGetInProgressDateTask() throws Exception {
        int taskID = 5350916;
        String jsonResponse = "[{\"values_diff\":{\"status\":[\"New\",\"In progress\"],\"taskboard_order\":[1707798961739,0]},\"created_at\":\"2024-02-18T22:53:09.286Z\"},{\"values_diff\":{\"status\":[\"New\",\"In progress\"],\"taskboard_order\":[1707798961739,0]},\"created_at\":\"2024-02-18T22:52:57.810Z\"}]";

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse httpResponse = mock(HttpResponse.class);
        HttpEntity httpEntity = new StringEntity(jsonResponse);

        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(httpResponse.getStatusLine())
                .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpURLConnection.HTTP_OK, "OK"));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);

        CycleTimeService cycleTimeService = new CycleTimeService();
        cycleTimeService.httpClient = httpClient;

        LocalDate inProgressDate = cycleTimeService.getInProgressDateTask(taskID);
        assertNotNull(inProgressDate);
        assertEquals(LocalDate.parse("2024-02-18"), inProgressDate);
    }

    
    // @Test
    // public void testGetTaskCycleTime() throws Exception {
    //     int milestoneID = 5468148;
    //     String jsonResponse = "[{\"id\":5350912,\"is_closed\":true,\"finished_date\":\"2024-02-18T22:55:24.441Z\"},{\"id\":5350913,\"is_closed\":false,\"finished_date\":\"2024-02-18T22:55:24.441Z\"},{\"id\":5361005,\"is_closed\":true,\"finished_date\":\"2024-02-18T22:55:25.714Z\"}]";

    //     HttpClient httpClient = mock(HttpClient.class);
    //     HttpResponse httpResponse = mock(HttpResponse.class);
    //     HttpEntity httpEntity = new StringEntity(jsonResponse);

    //     when(httpResponse.getEntity()).thenReturn(httpEntity);
    //     when(httpResponse.getStatusLine())
    //             .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpURLConnection.HTTP_OK, "OK"));
    //     when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);

    //     CycleTimeService cycleTimeService = new CycleTimeService();
    //     CycleTimeService mockCycleTimeService = mock(CycleTimeService.class);
    //     when(mockCycleTimeService.getInProgressDateTask(anyInt())).thenReturn(LocalDate.now());
    //     cycleTimeService.httpClient = httpClient;

    //     List<TaskDTO> tasks = cycleTimeService.getTaskCycleTime(milestoneID);
    //     assertNotNull(tasks);
    // }
}