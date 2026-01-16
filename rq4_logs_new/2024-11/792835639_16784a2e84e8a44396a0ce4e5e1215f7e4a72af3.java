package com.jinius.ecommerce.product.api;

import com.jinius.ecommerce.product.domain.Top5Product;
import lombok.*;

import java.math.BigInteger;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class TopProductResponse {
    private Long productId;
    private String productName;
    private BigInteger productPrice;
    private Long totalQuantity;

    public static TopProductResponse from(Top5Product top5Product) {
        return TopProductResponse.builder()
                .productId(top5Product.getId())
                .productName(top5Product.getName())
                .productPrice(top5Product.getPrice())
                .totalQuantity(top5Product.getTotalQuantity())
                .build();
    }
}