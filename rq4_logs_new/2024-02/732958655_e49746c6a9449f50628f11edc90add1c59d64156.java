package com.challenger.fridge.dto.item.request;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Data
public class StorageItemUpdateRequest {
    @Schema(description = "수정할 아이템 개수")
    private Long itemCount;
    @Schema(description = "수정할 아이템 설명")
    private String itemDescription;
    @Schema(description = "바꿀 유통기한")
    private String expireDate;
    @Schema(description = "바꿀 구매날짜")
    private String purchaseDate;
    @Hidden
    public LocalDate getExpireDateAsLocalDate() {
        return LocalDate.parse(expireDate, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    @Hidden
    public LocalDate getPurchaseDateAsLocalDate() {
        return  LocalDate.parse(purchaseDate, DateTimeFormatter.ISO_LOCAL_DATE);
    }
}