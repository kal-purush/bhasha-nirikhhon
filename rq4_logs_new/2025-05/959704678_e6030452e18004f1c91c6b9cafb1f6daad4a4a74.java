package com.example.docconneting.domain.alarm.entity;

import com.example.docconneting.domain.alarm.enums.AlarmType;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Table(name = "alarm_histories")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EntityListeners(AuditingEntityListener.class)
public class AlarmHistories {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String content;

    private Long toId;

    @Enumerated(EnumType.STRING)
    private AlarmType alarmType;

    @CreatedDate
    @Column(updatable = false)
    private LocalDateTime createdAt;

    private AlarmHistories(String content, Long toId, AlarmType alarmType) {
        this.content = content;
        this.toId = toId;
        this.alarmType = alarmType;
    }

    public static AlarmHistories of(String content, Long toId, AlarmType alarmType) {
        return new AlarmHistories(content, toId, alarmType);
    }
}
