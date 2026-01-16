package com.reddot.app.dto.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NonNull;

import java.util.HashSet;
import java.util.Set;

@Data
public class QuestionUpdateDTO {
    @JsonIgnore
    Integer id;
    @NonNull
    @Size(min = 3, max = 100)
    String title;
    @NonNull
    String body;
    Set<String> tags = new HashSet<>();

    // TODO: implement mod review before updating
    String comment;
}