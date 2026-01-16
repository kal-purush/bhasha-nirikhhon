package com.Kidari.server.domain.item.dto;

import com.Kidari.server.common.response.ApiResponse;
import com.Kidari.server.common.response.exception.ErrorCode;
import com.Kidari.server.common.response.exception.ItemException;
import com.Kidari.server.domain.item.entity.Item;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ItemDto {
    private Long snowId;
    private Long hatId;
    private Long decoId;

    public ItemDto(Item item) {
        this.snowId = item.getSnowId();
        this.hatId = item.getHatId();
        this.decoId = item.getDecoId();
    }

    public static Item toEntity(ItemDto itemDto, Item item) throws ItemException {
        if (item == null)
            throw new ItemException(ErrorCode.ITEM_NOT_FOUND);
        if (itemDto.getSnowId() < 0 || itemDto.getSnowId() >= 5
                ||  itemDto.getHatId() < 0 || itemDto.getHatId() >= 5
                ||  itemDto.getDecoId() < 0 || itemDto.getDecoId() >= 5) {
            throw new ItemException(ErrorCode.ITEM_BAD_REQUEST);
        }
        return Item.builder()
                .id(item.getId())
                .snowId(itemDto.getSnowId())
                .hatId(itemDto.getHatId())
                .decoId(itemDto.getDecoId())
                .build();
    }
}