package com.project.quanlycanghangkhong.dto;

import com.project.quanlycanghangkhong.model.SharePermission;
import java.time.LocalDateTime;

public class FileShareDTO {
    private Integer id;
    private Integer attachmentId;
    private String fileName;
    private String filePath;
    private Long fileSize;
    private UserDTO sharedBy;
    private UserDTO sharedWith;
    private SharePermission permission;
    private LocalDateTime sharedAt;
    private LocalDateTime expiresAt;
    private String note;
    private boolean isActive;
    private boolean isExpired;

    // Constructors
    public FileShareDTO() {}

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAttachmentId() {
        return attachmentId;
    }

    public void setAttachmentId(Integer attachmentId) {
        this.attachmentId = attachmentId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public UserDTO getSharedBy() {
        return sharedBy;
    }

    public void setSharedBy(UserDTO sharedBy) {
        this.sharedBy = sharedBy;
    }

    public UserDTO getSharedWith() {
        return sharedWith;
    }

    public void setSharedWith(UserDTO sharedWith) {
        this.sharedWith = sharedWith;
    }

    public SharePermission getPermission() {
        return permission;
    }

    public void setPermission(SharePermission permission) {
        this.permission = permission;
    }

    public LocalDateTime getSharedAt() {
        return sharedAt;
    }

    public void setSharedAt(LocalDateTime sharedAt) {
        this.sharedAt = sharedAt;
    }

    public LocalDateTime getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(LocalDateTime expiresAt) {
        this.expiresAt = expiresAt;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public boolean isExpired() {
        return isExpired;
    }

    public void setExpired(boolean expired) {
        isExpired = expired;
    }
}