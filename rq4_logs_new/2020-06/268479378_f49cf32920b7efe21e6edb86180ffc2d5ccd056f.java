package com.codecool.ccms.models.factory;

public class Attendance {
    private final int student_ID;
    private final int attendance_ID;
    private final int presence;

    public Attendance(int student_ID, int attendance_ID, int presence) {
        this.student_ID = student_ID;
        this.attendance_ID = attendance_ID;
        this.presence = presence;
    }

    public int getStudent_ID() {
        return student_ID;
    }

    public int getAttendance_ID() {
        return attendance_ID;
    }

    public int getPresence() {
        return presence;
    }
}