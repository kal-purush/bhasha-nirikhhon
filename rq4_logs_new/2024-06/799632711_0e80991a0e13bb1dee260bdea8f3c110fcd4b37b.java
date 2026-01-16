package com.LuuQu.medicalclinic.model.dto;

import lombok.Data;

import java.time.LocalDate;

@Data
public class PatientSimpleDto {
    private Long id;
    private String email;
    private String password;
    private String idCardNo;
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private LocalDate birthday;
}