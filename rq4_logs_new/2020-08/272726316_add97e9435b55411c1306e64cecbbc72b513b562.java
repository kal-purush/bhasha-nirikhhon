package com.gslab.talent.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name="panel")
public class Panel {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
    int id;
	
	
	
	@ManyToOne
	@JoinColumn(name = "candidateId")
	private Candidate candidate;
	
	
	Panel(){
		super();
	}
	
	
}