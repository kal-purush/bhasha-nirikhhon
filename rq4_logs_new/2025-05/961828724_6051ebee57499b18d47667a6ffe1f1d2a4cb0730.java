package com.project.quanlycanghangkhong.dto;

import java.util.List;

public class CreateDocumentRequest {
    private String documentType;
    private String content;
    private String notes;
    private List<AttachmentDTO> attachments;
    // getters and setters
    public String getDocumentType() { return documentType; }
    public void setDocumentType(String documentType) { this.documentType = documentType; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public String getNotes() { return notes; }
    public void setNotes(String notes) { this.notes = notes; }
    public List<AttachmentDTO> getAttachments() { return attachments; }
    public void setAttachments(List<AttachmentDTO> attachments) { this.attachments = attachments; }
}