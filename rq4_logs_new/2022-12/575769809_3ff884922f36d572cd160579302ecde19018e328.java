package com.project.attendancemanagementsystem.service;

import com.project.attendancemanagementsystem.entity.Schedule;
import com.project.attendancemanagementsystem.model.StudentRequest;
import com.project.attendancemanagementsystem.repository.ScheduleRepository;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;

@Service
public class ScheduleService {

    private final ScheduleRepository scheduleRepository;

    public ScheduleService(ScheduleRepository scheduleRepository) {
        this.scheduleRepository = scheduleRepository;
    }

    public Schedule findScheduleById(StudentRequest request) {
        return scheduleRepository.findById(request.getScheduleId())
                .orElseThrow(() -> new EntityNotFoundException("Schedule doesn't exist"));
    }
}