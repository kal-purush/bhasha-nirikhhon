package com.example.auth_service.dto;

import jakarta.validation.constraints.FutureOrPresent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TaskCreateDTO {

    @NotBlank
    @Size(min = 3, max = 255)
    private String title;

    @Size(max = 2000)
    private String description;

    @FutureOrPresent
    private LocalDateTime deadline;

    @NotNull
    private Long realEstateObjectId;
}