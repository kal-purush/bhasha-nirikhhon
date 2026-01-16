package com.example.timetracker.controller;

import com.example.timetracker.dto.WorkdayEntryReadDTO;
import com.example.timetracker.service.WorkdayEntryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/users/{userId}/workday-entries")
public class WorkdayEntryController {

    @Autowired
    private WorkdayEntryService workdayEntryService;

    @GetMapping
    public List<WorkdayEntryReadDTO> getUserWorkdayEntries(@PathVariable UUID userId) {
        return workdayEntryService.getUserWorkdayEntries(userId);
    }

    @PostMapping("/start")
    public WorkdayEntryReadDTO start(@PathVariable UUID userId) {
        return workdayEntryService.createWorkdayEntry(userId);
    }

    @PostMapping("/{workdayEntryId}/pause")
    public WorkdayEntryReadDTO pause(@PathVariable UUID userId, @PathVariable UUID workdayEntryId) {
        return workdayEntryService.pauseWorkdayEntry(userId, workdayEntryId);
    }

    @PostMapping("/{workdayEntryId}/resume")
    public WorkdayEntryReadDTO resume(@PathVariable UUID userId, @PathVariable UUID workdayEntryId) {
        return workdayEntryService.resumeWorkdayEntry(userId, workdayEntryId);
    }

    @PostMapping("/{workdayEntryId}/stop")
    public WorkdayEntryReadDTO stop(@PathVariable UUID userId, @PathVariable UUID workdayEntryId) {
        return workdayEntryService.stopWorkdayEntry(userId, workdayEntryId);
    }
}