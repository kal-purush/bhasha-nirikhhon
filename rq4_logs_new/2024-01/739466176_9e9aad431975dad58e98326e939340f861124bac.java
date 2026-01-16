package com.anghel.music_band_organizer.models.dtos;

import com.anghel.music_band_organizer.utils.enums.State;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RehearsalDTO {

    private Long id;

    private State state;

    @NotBlank
    private LocalDateTime rehearsalTime;

    @NotBlank
    @Min(1)
    @Max(9)
    private Integer rehearsalDurationHours;

    @NotBlank
    @Size(min = 3, max = 30, message = "must be between 3 and 30 characters")
    private String rehearsalBand;
}