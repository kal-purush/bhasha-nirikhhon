package com.example.sideproject.domain.notification.service;

import com.example.sideproject.domain.applicant.entity.ApplicationStatus;
import com.example.sideproject.domain.notification.aop.annotation.NotifyOn;
import com.example.sideproject.domain.notification.dto.EventDto;
import com.example.sideproject.domain.notification.dto.EventListDto;
import com.example.sideproject.domain.notification.entity.NotificationType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ApplicantNotificationService {

    /**
     * 지원자 등록 시 팀장에게 알림
     * @param projectId 프로젝트 고유번호
     * @param projectTitle 프로젝트명
     * @param position 지원한 직무
     * @param leaderId 팀장
     */
    @NotifyOn
    public EventListDto registerApplicant(Long projectId, String projectTitle, String position, Long leaderId) {
        String msg = """
                \'$_projectTitle\'의 \'[$_position]\'에 새로운 지원자가 있어요!
                """
                .replace("$_projectTitle", projectTitle)
                .replace("$_position", position);
        EventDto eventDto = EventDto.builder()
                .to(leaderId)
                .msg(msg)
                .relatedId(projectId)
                .type(NotificationType.PROJECT_APPLICANT)
                .build();
        return new EventListDto(List.of(eventDto));
    }

    /**
     * 지원서의 상태가 변경이 되면 해당 유저에게 알림
     * @param projectId 프로젝트 고유번호
     * @param projectTitle 프로젝트명
     * @param status 팀장이 변경한 상태
     * @param userId 지원자
     */
    @NotifyOn
    public EventListDto changeApplicantStatus(Long projectId, String projectTitle, ApplicationStatus status, Long userId) {
        String msg = """
                \'$_projectTitle\'에 \'$_status\'됐어요.
                """
                .replace("$_projectTitle", projectTitle)
                .replace("$_status", status.getDescription());
        EventDto eventDto = EventDto.builder()
                .to(userId)
                .msg(msg)
                .relatedId(projectId)
                .type(NotificationType.APPLICATION_RESULT)
                .build();
        return new EventListDto(List.of(eventDto));
    }
}