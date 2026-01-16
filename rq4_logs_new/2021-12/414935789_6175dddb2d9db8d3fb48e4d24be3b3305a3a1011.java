package com.kitchen.iChef.Controller.Model.Request;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

@Getter
@Setter
@ToString
public class FilterRequest {
    @NotBlank(message = "The field is invalid!")
    @Pattern(regexp = "title|rating|difficulty|preparationTime|portions", flags = Pattern.Flag.CASE_INSENSITIVE)
    private String field;
    @NotBlank(message = "The operation is invalid!")

    private OperationType operation;
    @NotBlank(message = "The text cannot be null!")
    private String text;

}