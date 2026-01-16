package com.example.backend.controller.response;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class ScheduleTeamResponseDTO {
    List<TeamScheduleResponseDTO> TeamSchedule;
}