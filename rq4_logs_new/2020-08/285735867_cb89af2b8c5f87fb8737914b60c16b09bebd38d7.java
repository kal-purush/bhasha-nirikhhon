package com.cc.pos.product.entity;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.DynamicUpdate;

@Entity
@Table(name="product",schema = "public")
@DynamicUpdate
public class Product implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6624104634020800494L;
	@Id
	private Long id;
	private String name;
	private Timestamp modifieddate;
	private String modifiedby;
	
	@Column( name = "createddate", nullable = false, updatable = false )
	private Timestamp createddate;
	@Column( name = "createdby", nullable = false, updatable = false )
	private String createdby;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Timestamp getModifieddate() {
		return modifieddate;
	}
	public void setModifieddate(Timestamp modifieddate) {
		this.modifieddate = modifieddate;
	}
	public Timestamp getCreateddate() {
		return createddate;
	}
	public void setCreateddate(Timestamp createddate) {
		this.createddate = createddate;
	}
	public String getModifiedby() {
		return modifiedby;
	}
	public void setModifiedby(String modifiedby) {
		this.modifiedby = modifiedby;
	}
	public String getCreatedby() {
		return createdby;
	}
	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}

	
	

}