package com.bnau.cdb.model;

import java.io.Serializable;
import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * Entity class for computer.
 *
 * @author bertrand
 *
 */
@Entity
@Table(name="computer")
public class Computer implements Serializable {
	
	/**
	 *
	 */
	private static final long serialVersionUID = 8960295030845081664L;
	private Long id;
	private String name;
	private LocalDateTime introduced;
	private LocalDateTime discontinued;
	private Company company;

	/**
	 * @return the id
	 */
	@Id
	@Column(name="id")
	public Long getId() {
		return this.id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(final Long id) {
		this.id = id;
	}

	/**
	 * @return the name
	 */
	@Column(name="name")
	public String getName() {
		return this.name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(final String name) {
		this.name = name;
	}

	/**
	 * @return the introduced
	 */
	@Column(name="introduced")
	public LocalDateTime getIntroduced() {
		return this.introduced;
	}

	/**
	 * @param introduced the introduced to set
	 */
	public void setIntroduced(final LocalDateTime introduced) {
		this.introduced = introduced;
	}

	/**
	 * @return the discontinued
	 */
	@Column(name="discontinued")
	public LocalDateTime getDiscontinued() {
		return this.discontinued;
	}

	/**
	 * @param discontinued the discontinued to set
	 */
	public void setDiscontinued(final LocalDateTime discontinued) {
		this.discontinued = discontinued;
	}

	/**
	 * @return the company
	 */
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="company_id")
	public Company getCompany() {
		return this.company;
	}

	/**
	 * @param company the company to set
	 */
	public void setCompany(final Company company) {
		this.company = company;
	}
}