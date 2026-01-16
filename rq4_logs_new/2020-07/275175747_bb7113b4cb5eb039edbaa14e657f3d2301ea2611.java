package jm.stockx.dto;

import jm.stockx.entity.BuyingInfo;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class BuyingInfoPutDto {

    private Long id;
    private LocalDateTime buyingTimeStamp;
    private Double buyingPrice;

    public BuyingInfoPutDto(BuyingInfo buyingInfo) {
        this.id = buyingInfo.getId();
        this.buyingTimeStamp = buyingInfo.getBuyingTimeStamp();
        this.buyingPrice = buyingInfo.getBuyingPrice();
    }

}