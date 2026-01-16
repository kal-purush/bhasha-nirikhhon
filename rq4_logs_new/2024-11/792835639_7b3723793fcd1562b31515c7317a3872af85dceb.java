package com.jinius.ecommerce.payment.domain.model;

import com.jinius.ecommerce.common.EcommerceException;
import com.jinius.ecommerce.common.ErrorCode;
import lombok.*;

import java.math.BigInteger;

import static com.jinius.ecommerce.payment.domain.model.PaymentType.POINT;

@Data
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class Payment {
    private Long orderId;
    private Long userId;
    private PaymentType type;
    private BigInteger amount;
    private BigInteger point;
    private PaymentStatus status;

    //결제 정보 생성
    public static Payment create(OrderPayment orderPayment) {
        //포인트 부족으로 결제 정보 생성 실패
        if (orderPayment.getType() == PaymentType.POINT
                && orderPayment.getUserPoint().compareTo(orderPayment.getOrderPrice()) <= 0) {
            throw new EcommerceException(ErrorCode.NOT_ENOUGH_POINT);
        }
        //주문 금액 0원 이하로 결제 정보 생성 실패
        if (orderPayment.getOrderPrice().compareTo(BigInteger.ZERO) <= 0)
            throw new EcommerceException(ErrorCode.INVALID_TOTAL_PRICE);

        return new Payment(
                orderPayment.getOrderId(),
                orderPayment.getUserId(),
                orderPayment.getType(),
                orderPayment.getType() == POINT ? BigInteger.ZERO : orderPayment.getOrderPrice(),
                orderPayment.getType() == POINT ? orderPayment.getOrderPrice() : BigInteger.ZERO,
                PaymentStatus.PNEDING
        );
    }
}