package com.anghel.music_band_organizer.models.dtos.band;

import com.anghel.music_band_organizer.models.entities.Post;
import com.anghel.music_band_organizer.models.entities.Rehearsal;
import com.anghel.music_band_organizer.models.entities.User;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GetAllBandsDTO {

    private Long id;

    @NotBlank
    @Size(min = 3, max = 30, message = "must be between 3 and 30 characters")
    private String bandName;

    @NotBlank
    @Size(min = 3, max = 200, message = "must be between 3 and 30 characters")
    private String bandDescription;

    @JsonIgnore
    private List<Post> postList = new ArrayList<>();

    @JsonIgnore
    private List<Rehearsal> rehearsalList = new ArrayList<>();

    @JsonIgnore
    private List<User> userList = new ArrayList<>();
}