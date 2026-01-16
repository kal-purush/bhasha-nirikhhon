package com.umc.bwither.user.dto;

import com.umc.bwither.breeder.entity.enums.Animal;
import com.umc.bwither.breeder.entity.enums.EmploymentStatus;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BreederDTO {
    private Animal animal;
    private String species;
    private String tradeName;
    private String representative;
    private String registrationNumber;
    private String licenseNumber;
    private String snsAddress;
    private String animalHospital;
    private EmploymentStatus employmentStatus;
    private Integer trustLevel;
    private String description;
}