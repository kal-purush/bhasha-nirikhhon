package com.artiexh.api.service;

import com.artiexh.data.jpa.entity.OrderGroupEntity;
import com.artiexh.model.domain.OrderGroup;
import com.artiexh.model.rest.order.request.CheckoutRequest;
import com.artiexh.model.rest.order.request.PaymentQueryProperties;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

public interface OrderGroupService {

	OrderGroup checkout(long userId, CheckoutRequest request);

	Page<OrderGroup> getInPage(Specification<OrderGroupEntity> query, Pageable pageable);

	OrderGroup getById(Long orderGroupId);

	String payment(Long id, PaymentQueryProperties paymentQueryProperties, Long userId);

	String confirmPayment(PaymentQueryProperties paymentQueryProperties);

}