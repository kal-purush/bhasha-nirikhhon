package com.artiexh.model.rest.order.response;

import com.artiexh.model.domain.UserAddress;
import com.artiexh.model.rest.transaction.OrderTransactionResponse;
import com.artiexh.model.rest.user.UserOrderResponse;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.*;

import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CheckoutResponse {
	@JsonSerialize(using = ToStringSerializer.class)
	private Long id;
	private Set<UserOrderResponse> orders;
	private UserAddress shippingAddress;
	private OrderTransactionResponse currentTransaction;
}