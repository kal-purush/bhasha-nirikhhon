package com.springpro.hospital.ApiResponse;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ApiResponse {
    private boolean success;
    private String message;
    private Object data;

    // Constructor
    public ApiResponse(boolean success, String message, Object data) {
        this.success = success;
        this.message = message;
        this.data = data;
    }

    public ApiResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
        this.data = null;
    }


    // Getters and Setters

}