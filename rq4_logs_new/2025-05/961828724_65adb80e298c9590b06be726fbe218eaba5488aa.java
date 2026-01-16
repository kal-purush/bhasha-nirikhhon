package com.project.quanlycanghangkhong.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Schema(description = "Request để xác nhận upload file - hỗ trợ cả single và multiple files")
public class ConfirmFlexibleUploadRequest {
    
    @NotEmpty(message = "Danh sách attachment ID không được rỗng")
    @Schema(description = "Danh sách ID của attachment cần xác nhận upload", required = true)
    private List<Integer> attachmentIds;
    
    // Constructors
    public ConfirmFlexibleUploadRequest() {}
    
    public ConfirmFlexibleUploadRequest(List<Integer> attachmentIds) {
        this.attachmentIds = attachmentIds;
    }
    
    // Convenience constructor for single file
    public ConfirmFlexibleUploadRequest(Integer attachmentId) {
        this.attachmentIds = List.of(attachmentId);
    }
    
    // Helper methods
    public boolean isSingleFile() {
        return attachmentIds != null && attachmentIds.size() == 1;
    }
    
    public Integer getSingleAttachmentId() {
        if (isSingleFile()) {
            return attachmentIds.get(0);
        }
        throw new IllegalStateException("Request không phải single file");
    }
    
    // Getters and Setters
    public List<Integer> getAttachmentIds() {
        return attachmentIds;
    }
    
    public void setAttachmentIds(List<Integer> attachmentIds) {
        this.attachmentIds = attachmentIds;
    }
}