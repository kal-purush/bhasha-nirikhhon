package com.artiexh.data.jpa.entity;

import io.hypersistence.utils.hibernate.id.Tsid;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.util.LinkedHashSet;
import java.util.Set;

@Getter
@Setter
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "order_group")
@EntityListeners(AuditingEntityListener.class)
public class OrderGroupEntity extends BaseAuditEntity{
	@Id
	@Tsid
	@Column(name = "id", nullable = false)
	private Long id;

	@OneToMany
	@JoinColumn(name = "order_group_id")
	private Set<OrderEntity> orders = new LinkedHashSet<>();

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "shipping_address_id", nullable = false)
	private UserAddressEntity shippingAddress;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "user_id", nullable = false)
	private UserEntity user;

	@OneToMany
	@JoinColumn(name = "order_group_id")
	private Set<OrderTransactionEntity> orderTransaction = new LinkedHashSet<>();
}