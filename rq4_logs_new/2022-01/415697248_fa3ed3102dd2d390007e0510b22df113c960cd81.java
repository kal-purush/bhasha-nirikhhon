package com.project.medicumzone.ui.model.request;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;

@Getter
@Setter
public class NewClientRequestModel {

    @NotBlank(message = "First name can not be blank !")
    private String firstName;

    @NotBlank(message = "Last name can not be blank !")
    private String lastName;

    @NotBlank(message = "Phone number name can not be blank !")
    private String phoneNumber;

    @Email(message = "Wrong email adress")
    @NotBlank(message = "Email can not be blank !")
    private String email;
//TODO: add exception handling
}