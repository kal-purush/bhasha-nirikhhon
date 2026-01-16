package pintoss.giftmall.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum OrderStatus {

    ORDER_RECEIVED("주문접수"),
    ORDER_CONFIRMED("주문확인"),
    PREPARING_SHIPMENT("상품준비중"),
    READY_FOR_SHIPMENT("배송준비중"),
    SHIPPED("배송중"),
    DELIVERED("배송완료"),
    CANCELLED("주문취소"),
    RETURN_REQUESTED("반품신청"),
    RETURN_IN_PROGRESS("반품처리중"),
    RETURNED("반품완료"),
    EXCHANGE_REQUESTED("교환신청"),
    EXCHANGE_IN_PROGRESS("교환처리중"),
    EXCHANGED("교환완료");

    private final String description;

}