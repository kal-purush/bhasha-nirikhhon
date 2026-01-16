package com.jinius.ecommerce.product.api.dto;

import com.jinius.ecommerce.product.application.dto.ProductDto;
import com.jinius.ecommerce.product.domain.model.Product;
import lombok.*;

import java.math.BigInteger;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class ProductResponse {
    private Long productId;
    private String productName;
    private BigInteger productPrice;
    private Long stockQuantity;

    public static ProductResponse from(ProductDto productDto) {
        return ProductResponse.builder()
                .productId(productDto.getProductId())
                .productName(productDto.getProductName())
                .productPrice(productDto.getProductPrice())
                .stockQuantity(productDto.getQuantity())
                .build();
    }
}