package org.example.JavaTaigaCode.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.example.JavaTaigaCode.models.BurndownChartDTO;
import org.example.JavaTaigaCode.models.MilestoneDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.net.HttpURLConnection;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:config-test.properties")
public class BurndownChartServiceTest {
    @Test
    public void testCalculateTotalRunningSum_Successful() throws Exception {
        // Arrange
        int milestoneID = 123;
        String jsonResponse = "{\"id\":123,\"name\":\"Test Milestone\",\"total_points\":10.0,"
                + "\"estimated_start\":\"2024-01-01\",\"estimated_finish\":\"2024-01-10\","
                + "\"user_stories\":[{\"is_closed\":true,\"finish_date\":\"2024-01-03T12:00:00Z\",\"total_points\":2.0},"
                + "{\"is_closed\":true,\"finish_date\":\"2024-01-05T12:00:00Z\",\"total_points\":3.0},"
                + "{\"is_closed\":true,\"finish_date\":\"2024-01-08T12:00:00Z\",\"total_points\":1.0}]}";
        ObjectMapper objectMapper = new ObjectMapper();
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse httpResponse = mock(HttpResponse.class);
        HttpEntity httpEntity = new StringEntity(jsonResponse);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(httpResponse.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpURLConnection.HTTP_OK, "OK"));
        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
        BurndownChart burndownChart = new BurndownChart();
        burndownChart.httpClient = httpClient;

        // Act
        MilestoneDTO milestone = burndownChart.calculateTotalRunningSum(milestoneID);

        // Assert
        assertNotNull(milestone);
        assertEquals(123, milestone.getMilestoneID());
        assertEquals("Test Milestone", milestone.getMilestoneName());
        assertEquals(10.0, milestone.getTotalPoints());
        assertEquals(LocalDate.parse("2024-01-01"), milestone.getStart_date());
        assertEquals(LocalDate.parse("2024-01-10"), milestone.getEnd_date());
        List<BurndownChartDTO> totalSumValue = milestone.getTotalSumValue();
        assertNotNull(totalSumValue);
        assertEquals(4, totalSumValue.size());
        Map<LocalDate, Double> expectedPointsMap = new TreeMap<>();
        expectedPointsMap.put(LocalDate.parse("2024-01-03"), 2.0);
        expectedPointsMap.put(LocalDate.parse("2024-01-05"), 3.0);
        expectedPointsMap.put(LocalDate.parse("2024-01-08"), 1.0);
    }
}