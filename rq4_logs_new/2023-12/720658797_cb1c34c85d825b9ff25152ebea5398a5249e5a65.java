package com.jeontongju.product.dynamodb.domian;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.jeontongju.product.config.DynamoDBConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@DynamoDBTable(tableName = "product_metrics_record")
public class ProductMetrics {

    @DynamoDBHashKey(attributeName = "product_id")
    private String productId;

    @DynamoDBAttribute(attributeName = "review_count")
    private Long reviewCount;

    @DynamoDBAttribute(attributeName = "total_sales_count")
    private Long totalSalesCount;

    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = DynamoDBConfig.LocalDateTimeConverter.class)
    private LocalDateTime createdAt;

    public void setReviewCount(Long reviewCount) {
        this.reviewCount = reviewCount;
    }

    public void setTotalSalesCount(Long totalSalesCount) {
        this.totalSalesCount = totalSalesCount;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}