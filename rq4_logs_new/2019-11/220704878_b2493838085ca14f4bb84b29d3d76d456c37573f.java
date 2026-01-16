package com.rest.springboot.models;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Table(name = "cidade")
public class Cidade {
	
	@Id
	@EqualsAndHashCode.Include
	private Long id;
	private String nome;
	@ManyToOne
	@JoinColumn(name = "estado_id")
	private Estado estado;
}