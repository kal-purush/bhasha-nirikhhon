package com.artiexh.data.jpa.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Size;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Entity
@EntityListeners(AuditingEntityListener.class)
@Table(name = "order_history")
public class OrderHistoryEntity {
	@EmbeddedId
	private OrderHistoryEntityId id;

	@Column(name = "datetime", nullable = false, updatable = false)
	@CreatedDate
	private LocalDateTime datetime;

	@Size(max = 255)
	@Column(name = "message")
	private String message;

}