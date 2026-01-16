package com.openlabs.shoppingmall.dto;

import com.openlabs.shoppingmall.entity.Items;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ItemTestResDto {
    /** 상품ID */
    private Long itemId;
    /** 상품명 */
    private String itemName;
    /** 상품가 */
    private String itemPrice;
    /** 재고 */
    private String itemStock;
    /** 할인률 */
    private Integer discountRate;
    /** 이벤트시작일시 */
    private LocalDateTime eventStartTime;
    /** 이벤트종료일시 */
    private LocalDateTime eventEndTime;

    public Items saveEntity(){
        return Items.builder()
                .itemName(this.itemName)
                .itemPrice(this.itemPrice)
                .itemStock(this.itemStock)
                .discountRate(this.discountRate)
                .eventStartTime(this.eventStartTime)
                .eventEndTime(this.eventEndTime)
                .build();
    }
}