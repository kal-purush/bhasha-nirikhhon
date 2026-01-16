package com.whitedisk.white_disk.dto.file;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/**
 * @author white
 */
@Data
@Schema(name = "批量删除回收文件DTO",required = true)
public class BatchDeleteRecoveryFileDTO {
    @Schema(description="用户文件Id集合", required = true)
    private String userFileIds;
}