package com.example.mealme.vo;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;


@Component
@Data
@NoArgsConstructor
public class CompanyListVo {
   private String companyName;
   private String companyCeoName;
   private String companyCallNumber;
   private String companyAddress1;
   private Long companyNumber;
   private String companyRegistrationNumber;
   private String companyUrl;
   private String companyComment;
   private String companyEmail;
   private Long companyCodeNumber;
   private String companyCodeName;
   private double reviewGrade;
   private double averageGrade;
   private Long fileNumber;
   private String fileName;
   private String fileUploadPath;
   private String fileUuid;
}