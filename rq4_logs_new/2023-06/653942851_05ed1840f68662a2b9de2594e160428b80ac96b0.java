package com.example.mealme.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@Data
@NoArgsConstructor
public class CompanyDto {
    private long companyNumber;
    private long companyCodeNumber;
    private long companyRegistrationNumber;
    private String companyId;
    private String companyPassword;
    private String companyName;
    private String companyCeoName;
    private String companyCallNumber;
    private String companyAddressNumber;
    private String companyAddress1;
    private String companyAddress2;
    private String companyUrl;
    private String companyEmail;
    private String companyComment;
}