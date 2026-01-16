package com.openlabs.shoppingmall.dto;

import com.openlabs.shoppingmall.entity.Items;
import com.openlabs.shoppingmall.entity.OrderItem;
import com.openlabs.shoppingmall.entity.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderTestResDto {
    /** 주문ID */
    private Long orderId;
    /** 주문일시 */
    private LocalDateTime orderDate;
    /** 주문상태 */
    private OrderStatus orderStatus;

    /** 고객 연관관계 */
//    private Users users;
    /** 주문상품 연관관계 */
    private List<OrderItem> orderItem = new ArrayList<>();

    private Items item;
}