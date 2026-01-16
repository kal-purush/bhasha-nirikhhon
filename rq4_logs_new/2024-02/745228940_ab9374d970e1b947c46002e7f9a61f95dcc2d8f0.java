package org.example.JavaTaigaCode.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.example.JavaTaigaCode.models.TaskDTO;
import org.example.JavaTaigaCode.util.GlobalData;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class FoundWorkService {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private final String TAIGA_API_ENDPOINT = GlobalData.getTaigaURL();

    String datePattern = "yyyy-MM-dd";
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(datePattern);
    HttpClient httpClient = HttpClients.createDefault();

    public List<TaskDTO> getFoundWork(Integer milestoneID) {
        try{
            String response = "";
            String endpoint = TAIGA_API_ENDPOINT + "/milestones/" + milestoneID;
            HttpGet request = new HttpGet(endpoint);
            request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + Authentication.authToken);
            request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            HttpResponse httpResponse = httpClient.execute(request);
            int httpStatus = httpResponse.getStatusLine().getStatusCode();
            if (httpStatus < 200 || httpStatus >= 300) {
                throw new RuntimeException(httpResponse.getStatusLine().toString());
            }
            HttpEntity responseEntity = httpResponse.getEntity();
            if (responseEntity != null) {
                response = EntityUtils.toString(responseEntity);
            } else {
                throw new RuntimeException("Response body is NULL");
            }
            JsonNode milestoneJSON = objectMapper.readTree(response);
            LocalDate sprintStartDate = (LocalDate.parse(milestoneJSON.get("estimated_start").asText(), dateFormatter));

            // API call to get list of tasks created after sprint start date
            //https://api.taiga.io/api/v1/tasks?milestone=376605&created_date__gt=2024-02-20
            endpoint = TAIGA_API_ENDPOINT + "/tasks?milestone=" + milestoneID + "&created_date__gt=" + sprintStartDate.plusDays(1);
            request = new HttpGet(endpoint);
            request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + Authentication.authToken);
            request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            httpResponse = httpClient.execute(request);
            httpStatus = httpResponse.getStatusLine().getStatusCode();
            if (httpStatus < 200 || httpStatus >= 300) {
                throw new RuntimeException(httpResponse.getStatusLine().toString());
            }
            responseEntity = httpResponse.getEntity();
            if (responseEntity != null) {
                response = EntityUtils.toString(responseEntity);
            } else {
                throw new RuntimeException("Response body is NULL");
            }
            JsonNode taskListJSON = objectMapper.readTree(response);
            List<TaskDTO> tasks = new ArrayList<>();
            if(taskListJSON.isArray()) {
                for(JsonNode taskJSON: taskListJSON) {
                    TaskDTO taskDTO = new TaskDTO();
                    taskDTO.setTaskID(taskJSON.get("id").asInt());
                    LocalDateTime dateTime = LocalDateTime.parse(taskJSON.get("created_date").asText(),
                            DateTimeFormatter.ISO_DATE_TIME);
                    taskDTO.setCreatedDate(dateTime.toLocalDate());
                    tasks.add(taskDTO);
                }
            }
            return tasks;
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
}