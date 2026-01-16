package com.challenger.fridge.dto.item.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;
@Data
@Schema(description = "카테고리별 상품 리스트")
public class CategoryStorageItemResponse {
    @Schema(description = "카테고리 이름")
    private String categoryName;
    @Schema(description = "세부 보관소 상품 리스트")
    private List<StorageItemResponse> storageItems;

    public CategoryStorageItemResponse(String categoryName, List<StorageItemResponse> storageItems) {
        this.categoryName = categoryName;
        this.storageItems = storageItems;
    }
}