package com.project.quanlycanghangkhong.dto.request;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

@Schema(description = "Request để gán các attachment vào document")
public class AttachmentAssignRequest {
    @Schema(description = "Danh sách id của attachment sẽ gán vào document", required = true)
    private List<Integer> attachmentIds;

    public List<Integer> getAttachmentIds() {
        return attachmentIds;
    }
    public void setAttachmentIds(List<Integer> attachmentIds) {
        this.attachmentIds = attachmentIds;
    }
}