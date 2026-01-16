package com.skilldistillery.giggity.entities;

import java.time.LocalDateTime;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

@Entity
public class Job {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int id;
	private int price;
	private String description;
	private String title;
	private boolean active;
	private boolean remote;

	@Column(name = "image_url")
	private String imageUrl;
	@Column(name = "date_created")
	private LocalDateTime dateCreated;
	@Column(name = "date_updated")
	private LocalDateTime dateUpdated;

	@OneToMany(mappedBy = "address")
	private List<Address> addresses;
	
	@OneToOne
	@JoinColumn(name = "job_id")
	private BuyerReview buyerReview;
}