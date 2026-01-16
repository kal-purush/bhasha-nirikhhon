package com.example.lifeonhana.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.math.BigDecimal;

@Entity
@Table(name = "account")
@Getter @Setter
@NoArgsConstructor
public class Account {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long accountId;

	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "mydata_id", nullable = false)
	private Mydata mydata;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private Bank bank;

	@Column(nullable = false)
	private String accountNumber;

	@Column(nullable = false)
	private String accountName;

	@Column(nullable = false, precision = 8, scale = 2)
	private BigDecimal balance;

	@Column(nullable = false)
	private Boolean isMain;

	public enum Bank {
		HANA,
		KB,
		SHINHAN,
		WOORI,
		NH,
		IBK
	}
}