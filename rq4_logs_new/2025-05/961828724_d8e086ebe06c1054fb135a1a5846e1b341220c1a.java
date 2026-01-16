package com.project.quanlycanghangkhong.dto;

import java.time.LocalDateTime;

public class AssignmentStatusHistoryDTO {
    private Long id;
    private Long assignmentId;
    private Long userId;
    private String status;
    private String comment;
    private String attachmentUrl;
    private LocalDateTime changedAt;

    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public Long getAssignmentId() {
        return assignmentId;
    }
    public void setAssignmentId(Long assignmentId) {
        this.assignmentId = assignmentId;
    }
    public Long getUserId() {
        return userId;
    }
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getComment() {
        return comment;
    }
    public void setComment(String comment) {
        this.comment = comment;
    }
    public String getAttachmentUrl() {
        return attachmentUrl;
    }
    public void setAttachmentUrl(String attachmentUrl) {
        this.attachmentUrl = attachmentUrl;
    }
    public LocalDateTime getChangedAt() {
        return changedAt;
    }
    public void setChangedAt(LocalDateTime changedAt) {
        this.changedAt = changedAt;
    }
}