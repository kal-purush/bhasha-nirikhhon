package com.web.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Entity
@SequenceGenerator(name="COUNTINGSTAR_SEQ_GENERATOR", sequenceName="COUNTINGSTAR_SEQ", allocationSize = 1)
@Table(name="countingstar")
public class CountingStar {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "COUNTINGSTAR_SEQ_GENERATOR")
	private Long seq;
	
	private String username;
	private String jmnm;
	
	
	public CountingStar() {	
    }


	public CountingStar(Long seq, String username, String jmnm) {
		this.seq = seq;
		this.username = username;
		this.jmnm = jmnm;
	}
	
	
	
}