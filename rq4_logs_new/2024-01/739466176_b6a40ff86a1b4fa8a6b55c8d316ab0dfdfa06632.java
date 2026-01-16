package com.anghel.music_band_organizer.models.dtos;

import com.anghel.music_band_organizer.models.entities.Band;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class PostDTO {

    private Long id;

    @NotBlank
    @Size(min = 3, max = 30, message = "Must be between 3 and 30 characters.")
    private String postTitle;

    @NotBlank
    @Size(min = 3, max = 1000, message = "Must be between 3 and 1000 characters.")
    private String postDescription;

    @NotBlank
    @Size(min = 3, max = 30, message = "Must be between 3 and 30 characters.")
    private String postGenre;

    private Band postBand;
}