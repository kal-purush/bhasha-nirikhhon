package com.spotifyapi.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ErrorResponseDTO {

    private int status;
    private String error;
    private String message;
}