package com.example.schedule.domain.patient.dto.requestDto;

import com.example.schedule.global.globalEnum.Gender;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PatientUpdateRequestDto {
    private String name;
    private int age;
    private String dx;
    private int physical;
    private LocalDate onset;
    private LocalDate createAt;
    private int nonManner;
    private Gender gender;
}