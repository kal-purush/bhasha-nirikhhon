package com.example.backend.controller;

import com.example.backend.controller.request.CreateScheduleRequestDTO;
import com.example.backend.controller.request.ModifyScheduleRequestDTO;
import com.example.backend.controller.response.Response;
import com.example.backend.controller.response.ScheduleDetailResponseDTO;
import com.example.backend.entity.TeamEntity;
import com.example.backend.entity.TeamScheduleEntity;
import com.example.backend.service.ScheduleService;
import lombok.RequiredArgsConstructor;
import org.apache.ibatis.javassist.NotFoundException;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// 팀 스케줄 관련 api
@RestController
@RequestMapping("/api/schedule/team")
@RequiredArgsConstructor
public class TeamScheduleController {
    private final ScheduleService scheduleService;

    @PostMapping("")
    public Response<Void> createSchedule(@RequestBody CreateScheduleRequestDTO requestDTO, TeamEntity team) throws NotFoundException {
        scheduleService.createTeamSchedule(requestDTO, team.getId());
        return Response.success();
    }

    @DeleteMapping("/{scheduleId}")
    public Response<Void> deleteSchedule(@PathVariable Long scheduleId, TeamEntity team) throws NotFoundException {
        scheduleService.deleteTeamSchedule(scheduleId, team.getId());
        return Response.success();
    }

    @PutMapping("/{scheduleId}")
    public Response<Void> modifySchedule(@PathVariable Long scheduleId, TeamEntity team, @RequestBody ModifyScheduleRequestDTO requestDTO) throws NotFoundException {
        scheduleService.modifyTeamSchedule(scheduleId, team.getId(), requestDTO);
        return Response.success();
    }

    @GetMapping("/{scheduleId}")
    public Response<ScheduleDetailResponseDTO> getScheduleDetail(TeamEntity team, @PathVariable Long scheduleId) throws NotFoundException {
        return Response.success(scheduleService.getTeamDetail(team.getId(), scheduleId));
    }

    // 투두 반환
    @GetMapping("/Todolist")
    public List<TeamScheduleEntity> getToDoList() {
        return scheduleService.getToDoList();
    }
}