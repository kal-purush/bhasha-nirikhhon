package ru.practicum.gateway.clientTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import ru.practicum.gateway.client.BaseClient;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class BaseClientTest {

    @Mock
    private RestTemplate rest;
    private BaseClient baseClient;
    private static final String path = "/api/test";
    private static final long userId = 123;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        baseClient = new BaseClient(rest);
        ReflectionTestUtils.setField(baseClient, "rest", rest);
    }

    @Test
    void testGetWithNoParameters() {
        when(rest.exchange(eq(path), eq(HttpMethod.GET), any(), eq(Object.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        ResponseEntity<Object> response = baseClient.get(path);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void testGetWithUserIdAndParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", "value2");

        when(rest.exchange(eq(path), eq(HttpMethod.GET), any(), eq(Object.class), eq(parameters)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        ResponseEntity<Object> response = baseClient.get(path, userId, parameters);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void testPostWithUserIdAndBody() {
        Object body = new Object();

        when(rest.exchange(eq(path), eq(HttpMethod.POST), any(), eq(Object.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.CREATED));

        ResponseEntity<Object> response = baseClient.post(path, userId, body);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void testPutWithUserIdAndParametersAndBody() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", "value2");
        Object body = new Object();

        when(rest.exchange(eq(path), eq(HttpMethod.PUT), any(), eq(Object.class), eq(parameters)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        ResponseEntity<Object> response = baseClient.put(path, userId, parameters, body);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void testPatchWithNoUserId() {
        when(rest.exchange(eq(path), eq(HttpMethod.PATCH), any(), eq(Object.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.NO_CONTENT));
        Object body = new Object();
        ResponseEntity<Object> response = baseClient.patch(path, body);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void testDeleteWithUserIdAndParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", "value2");

        when(rest.exchange(eq(path), eq(HttpMethod.DELETE), any(), eq(Object.class), eq(parameters)))
                .thenReturn(new ResponseEntity<>(HttpStatus.NO_CONTENT));

        ResponseEntity<Object> response = baseClient.delete(path, userId, parameters);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertNull(response.getBody());
    }
}