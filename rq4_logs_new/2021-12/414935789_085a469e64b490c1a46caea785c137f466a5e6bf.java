package com.kitchen.iChef.Controller.Model.Request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

@Getter
@Setter
@ToString
public class SortingRequest {

    @NotBlank(message = "The field is not valid!")
    @Pattern(regexp = "title|rating|difficulty|preparationTime|portions|numberOfViews", flags = Pattern.Flag.CASE_INSENSITIVE)
    private String field;

    @JsonProperty(defaultValue = "true")
    private boolean ascending;
}