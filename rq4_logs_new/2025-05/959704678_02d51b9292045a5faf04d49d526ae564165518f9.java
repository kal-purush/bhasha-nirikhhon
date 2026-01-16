package com.example.docconneting.domain.alarm.dto;

import com.example.docconneting.domain.alarm.entity.AlarmHistories;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.List;

@Getter
public class AlarmResponse {

    private final Long id;
    private final String content;
    private final String alarmType;
    private final LocalDateTime createdAt;

    private AlarmResponse(Long id, String content, String alarmType, LocalDateTime createdAt) {
        this.id = id;
        this.content = content;
        this.alarmType = alarmType;
        this.createdAt = createdAt;
    }

    public static AlarmResponse of (Long id, String content, String alarmType, LocalDateTime createdAt) {
        return new AlarmResponse(id, content, alarmType, createdAt);
    }

    public static List<AlarmResponse> toAlarmResponse(List<AlarmHistories> alarmHistoriesList) {
        List<AlarmResponse> alarmResponseList = alarmHistoriesList.stream()
                .map(alarmHistories -> new AlarmResponse(
                        alarmHistories.getId(),
                        alarmHistories.getContent(),
                        alarmHistories.getAlarmType().name(),
                        alarmHistories.getCreatedAt()))
                .toList();
        return alarmResponseList;
    }
}