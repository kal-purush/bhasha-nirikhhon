package com.jinius.ecommerce.payment.domain;

import com.jinius.ecommerce.common.EcommerceException;
import com.jinius.ecommerce.common.ErrorCode;
import com.jinius.ecommerce.order.domain.Order;
import com.jinius.ecommerce.user.domain.User;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.math.BigInteger;

import static com.jinius.ecommerce.order.domain.OrderStatus.PENDING;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;


    /**
     * 결제
     * @param user
     * @param order
     */
    public Payment pay(User user, Order order) {
        if (ObjectUtils.isEmpty(user))
            throw new EcommerceException(ErrorCode.INVALID_PARAMETER);

        if (ObjectUtils.isEmpty(order))
            throw new EcommerceException(ErrorCode.INVALID_PARAMETER);

        if (order.getTotalPrice().compareTo(BigInteger.ZERO) <= 0)
            throw new EcommerceException(ErrorCode.INVALID_PARAMETER);

        if (order.getOrderStatus() != PENDING)
            throw new EcommerceException(ErrorCode.ALREADY_PAID_ORDER);

        if (user.getPoint().compareTo(order.getTotalPrice()) < 0)
            throw new EcommerceException((ErrorCode.NOT_ENOUGH_POINT));

        //포인트 차감
        user.subtractPoint(order.getTotalPrice());

        //결제 정보 생성
        Payment payment = Payment.create(user, order);

        //저장
        return paymentRepository.save(payment);
    }
}