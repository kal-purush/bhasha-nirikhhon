package com.albydaa.equinefarm.dtos;
import com.albydaa.equinefarm.base.BaseEntity;
import com.albydaa.equinefarm.model.Horse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import com.albydaa.equinefarm.model.Doctor.Specialization;

import java.util.ArrayList;
import java.util.List;
@Setter
@Getter
@Builder
public class DoctorDTO {
    private Long id;
    private String firstName;
    private String lastName;
    private Double salary;
    private Specialization specialization;
}