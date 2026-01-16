package com.project.quanlycanghangkhong.dto.response.attachment;

import com.project.quanlycanghangkhong.dto.response.ApiResponseCustom;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Schema(description = "API response for bulk delete attachments", required = true)
public class ApiBulkDeleteAttachmentResponse extends ApiResponseCustom<String> {
    public ApiBulkDeleteAttachmentResponse(String message, int statusCode, String data, boolean success) {
        super(message, statusCode, data, success);
    }
}