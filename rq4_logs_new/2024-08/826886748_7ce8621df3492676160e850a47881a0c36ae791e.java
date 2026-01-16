package com.umc.bwither.breeder.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.Optional;

@Getter
@NoArgsConstructor
public class BreedingHistoryDTO {
    private String breedingTradeName;      // 상호명

    private String breedingJoinDate;       // 입사 연월 (예: YYYY-MM)

    private String breedingLeaveDate;     // 퇴사 연월 (예: YYYY-MM)

    private Boolean breedingCurrentlyEmployed; // 재직 중 여부
}