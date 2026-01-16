package com.commerce.core.product.entity.mongo;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * 상품 정보 View
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PRODUCT_VIEW")
public class ProductView {

    /**
     * 상품 정보 View Seq
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long productViewSeq;

    /**
     * 상품 정보 Seq
     */
    private Long productInfoSeq;

    /**
     * 상품명
     */
    private String productName;

    /**
     * 상품 상세 설명
     */
    private String productDetail;

    /**
     * 상품 가격
     */
    private Long price;

    /**
     * 상품 할인 가격 (실제 판매 가격)
     */
    private Long discountPrice;

    /**
     * 사용 여부 (Y / N)
     */
    private String useYn;

    /**
     * 상품 옵션
     */
    private List<String> productOptions;

    /**
     * 상품 상세 구분
     */
    private List<String> productDetailCodes;
}