package com.example.backend.entity;

import com.example.backend.controller.request.CreateScheduleRequestDTO;
import com.example.backend.controller.request.ModifyScheduleRequestDTO;
import com.example.backend.model.ScheduleType;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name = "team_schedule")
@NoArgsConstructor
@Getter
@DiscriminatorValue("TEAM")
public class TeamScheduleEntity extends ScheduleEntity {

    @Builder
    private TeamScheduleEntity(CreateScheduleRequestDTO requestDTO, TeamEntity team){
        super.title = requestDTO.getTitle();
        this.team = team;
        this.startMonth = requestDTO.getStartDate().getMonth();
        this.endMonth = requestDTO.getEndDate().getMonth();
        this.content = requestDTO.getContent();
        this.startDate = requestDTO.getStartDate();
        this.endDate = requestDTO.getEndDate();
        this.startYear = requestDTO.getStartDate().getYear();
        this.endYear = requestDTO.getEndDate().getYear();
    }

    public static TeamScheduleEntity fromTeamScheduleDTO(CreateScheduleRequestDTO requestDTO, TeamEntity team) {
        return new TeamScheduleEntity(requestDTO, team);
    }

    public void modifySchedule(ModifyScheduleRequestDTO requestDTO) {
        this.title = requestDTO.getTitle();
        this.startMonth = requestDTO.getStartDate().getMonth();
        this.endMonth = requestDTO.getEndDate().getMonth();
        this.content = requestDTO.getContent();
        this.startDate = requestDTO.getStartDate();
        this.endDate = requestDTO.getEndDate();
        this.startYear = requestDTO.getStartDate().getYear();
        this.endYear = requestDTO.getEndDate().getYear();
    }

}