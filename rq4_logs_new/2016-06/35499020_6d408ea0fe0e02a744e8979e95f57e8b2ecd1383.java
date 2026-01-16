package com.janprach.shopper.sreality.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.springframework.data.rest.core.annotation.RestResource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(exclude = "estate")
@Entity
@Table(indexes = { @Index(name = "HISTORY_BY_ESTATE_ID", columnList = "ESTATE_ID") })
public class History extends EntityBase {
	@RestResource(rel = "histories")
	@JsonIgnore
	@ManyToOne(optional = false)
	@JoinColumn(name = "ESTATE_ID")
	private Estate estate;

	@Column(nullable = false)
	@Enumerated(EnumType.STRING)
	private HistoryType historyType;

	@Column(nullable = false)
	private String message;
}