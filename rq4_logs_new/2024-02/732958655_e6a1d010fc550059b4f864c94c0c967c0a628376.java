package com.challenger.fridge.controller;

import com.challenger.fridge.dto.ApiResponse;
import com.challenger.fridge.dto.item.StorageItemDto;
import com.challenger.fridge.service.StorageBoxService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/storagebox")
@Tag(name = "storagebox", description = "세부보관소 API")
public class StorageBoxController {
    private final StorageBoxService storageBoxService;

    @PostMapping("/{storageBoxId}/items/new")
    @Operation(summary = "(검색 탭에서)세부 보관소에 상품 추가", description = "세부 보관소에 상품을 추가한다.(검색 탭에서 냉장고로 바로 추가)")
    public ApiResponse addStorageItemToStorageBox(@RequestBody StorageItemDto storageItemDto, @PathVariable("storageBoxId") Long storageBoxId) {
        storageBoxService.saveStorageItem(storageItemDto, storageBoxId);
        return ApiResponse.success(null);
    }


}