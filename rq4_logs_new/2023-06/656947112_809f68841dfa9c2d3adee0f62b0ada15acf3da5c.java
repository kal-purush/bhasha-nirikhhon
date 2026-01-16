package com.ar.conde.reservaDeTurnos.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;

import java.util.HashMap;
import java.util.Map;

public class CustomFieldException {
    public static ResponseEntity<Map<String, String>> validate(BindingResult result) {
        Map<String, String> erroresResult = new HashMap<>();
        result.getFieldErrors().forEach(err -> {
            erroresResult.put("error","El campo " + err.getField() + " " + err.getDefaultMessage());
        });
        return ResponseEntity.badRequest().body(erroresResult);
    }

    public static ResponseEntity<Map<String, String>> customResponseError(Exception e) {
        Map<String, String> errorResponse = new HashMap<>();
        errorResponse.put("error", e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}