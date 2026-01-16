package com.example.campuschool_backend.controller;

import com.example.campuschool_backend.dto.lecture.NotificationDTO;
import com.example.campuschool_backend.service.LectureService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@RequiredArgsConstructor
@RequestMapping("/api/user/open")
@RestController
public class NotificationController {
    private final LectureService lectureService;
    @GetMapping("/{id}/notification")
    public ResponseEntity<Page<NotificationDTO>> getNotifications(@PathVariable Long id, Pageable pageable) {
        Page<NotificationDTO> notificationDTOS = lectureService.getNotifications(id,pageable);
        return ResponseEntity.ok(notificationDTOS);
    }

}