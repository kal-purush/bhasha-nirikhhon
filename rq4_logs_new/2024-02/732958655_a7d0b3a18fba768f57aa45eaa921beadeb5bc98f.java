package com.challenger.fridge.dto.box.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
@Schema(description = "세부 보관소 추가 Request")
public class StorageBoxSaveRequest {
    @Schema(description = "세부 보관소의 이름")
    @NotEmpty(message = "세부 보관소의 이름을 필수로 입력하세요.")
    private String storageBoxName;

    @Schema(description = "보관방식(FRIDGE,FREEZE)")
    @NotNull(message = "보관방식은(FRIDGE,FREEZE)만 가능합니다.")
    private StorageMethod storageMethod;
}