package tabom.myhands.domain.schedule.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import lombok.*;
import org.antlr.v4.runtime.misc.NotNull;

import java.sql.Timestamp;

@Entity
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "schedule")
public class Schedule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="schedule_id", nullable = false)
    private Long scheduleId;

    private String title;

    @Column(name = "start_at")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss" )
    private Timestamp startAt;

    @Column(name = "finish_at")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss" )
    private Timestamp finishAt;

    private String place;

    @Column(name = "created_at")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss" )
    private Timestamp createdAt;

    private int category;성

    @Column(name = "user_id")
    private Long userId;
}