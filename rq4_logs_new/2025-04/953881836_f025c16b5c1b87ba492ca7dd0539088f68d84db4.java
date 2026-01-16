package com.nextstep.ansan_lms_back.entity;

import jakarta.persistence.*;

@Entity(name="Attendance")
@Table(name="Attendance")

public class Attendance {
    @Id
    private int attendanceId;
    private int timetableId;
    private int week;
    private int period;
    private int attendanceStatus;
}