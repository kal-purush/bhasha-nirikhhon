package com.brainstrom.meokjang.admin.auth.dto;

import lombok.Getter;

@Getter
public class HandleReportForm {

    private final Long reportId;
    private final int suspensionDays;
    private final String adminName;

    public HandleReportForm(Long reportId, int suspensionDays, String adminName) {
        this.reportId = reportId;
        this.suspensionDays = suspensionDays;
        this.adminName = adminName;
    }
}