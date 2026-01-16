package com.challenger.fridge.dto.box.response;

import com.challenger.fridge.domain.box.StorageBox;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(description = "세부 보관소 정보 Response")
public class StorageBoxResponse {
    @Schema(description = "세부 보관소 고유 ID")
    private Long storageBoxId;
    @Schema(description = "세부 보관소 이름")
    private String storageBoxName;

    public StorageBoxResponse(StorageBox storageBox) {
        this.storageBoxId = storageBox.getId();
        this.storageBoxName = storageBox.getName();

    }
}