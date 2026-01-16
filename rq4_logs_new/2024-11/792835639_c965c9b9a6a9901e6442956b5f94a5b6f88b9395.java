package com.jinius.ecommerce.payment.infra;

import com.jinius.ecommerce.payment.domain.model.OrderPaymentInfo;
import com.jinius.ecommerce.payment.domain.model.Payment;
import com.jinius.ecommerce.payment.domain.PaymentRepository;
import com.jinius.ecommerce.payment.infra.entity.PaymentEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class PaymentRepositoryImpl implements PaymentRepository {

    private final PaymentJpaRepository paymentJpaRepository;

    @Override
    public Payment save(OrderPaymentInfo orderPaymentInfo) {
        return paymentJpaRepository.save(PaymentEntity.fromOrderPaymentInfo(orderPaymentInfo)).toDomain();
    }

    @Override
    public void updateStatus(Payment payment) {
        paymentJpaRepository.updateStatus(payment.getPaymentId(), payment.getStatus());
    }
}