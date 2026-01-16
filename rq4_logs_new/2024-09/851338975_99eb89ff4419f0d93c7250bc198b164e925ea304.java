package com.reservibe.application.response;

import com.reservibe.domain.generic.output.OutputInterface;
import com.reservibe.domain.generic.output.OutputStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GenericResponseTest {

    private GenericResponse genericResponse;
    private OutputInterface outputInterface;

    @BeforeEach
    void setUp() {
        genericResponse = new GenericResponse();
        outputInterface = mock(OutputInterface.class);
    }

    @Test
    void testResponseOk() {
        OutputStatus status = new OutputStatus(200, "OK", "OK");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("OK");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("OK", response.getBody());
    }

    @Test
    void testResponseCreated() {
        OutputStatus status = new OutputStatus(201, "Created", "Created");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Created");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Created");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Created", response.getBody());
    }

    @Test
    void testResponseNotFound() {
        OutputStatus status = new OutputStatus(404, "Not Found", "Not Found");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Not Found");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals("Not Found", response.getBody());
    }

    @Test
    void testResponseNoContent() {
        OutputStatus status = new OutputStatus(204, "No Content", "No Content");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("No Content");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertEquals("No Content", response.getBody());
    }

    @Test
    void testResponseUnprocessableEntity() {
        OutputStatus status = new OutputStatus(422, "Unprocessable Entity", "Unprocessable Entity");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Unprocessable Entity");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.getStatusCode());
        assertEquals("Unprocessable Entity", response.getBody());
    }

    @Test
    void testResponseBadRequest() {
        OutputStatus status = new OutputStatus(400, "Bad Request", "Bad Request");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Bad Request");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals("Bad Request", response.getBody());
    }

    @Test
    void testResponseInternalServerError() {
        OutputStatus status = new OutputStatus(500, "Internal Server Error", "Internal Server Error");
        when(outputInterface.getOutputStatus()).thenReturn(status);
        when(outputInterface.getBody()).thenReturn("Internal Server Error");

        ResponseEntity<Object> response = genericResponse.response(outputInterface);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertEquals("Internal Server Error", response.getBody());
    }
}