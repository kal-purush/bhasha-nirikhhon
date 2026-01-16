package com.cc.pos.product.entity;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;



@Entity
@Table(name = "product_category")
public class ProductCategory {
	
	
	@Id
	private Long id;
	private String name;
	private String type;
	
	 @ManyToMany(mappedBy = "pcSet", cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
	    private Set<ProductLineItem> pliSet = new HashSet<>();

	
	 
	 
	private Timestamp modifieddate;
	private String modifiedby;
	@Column( name = "createddate", nullable = false, updatable = false )
	private Timestamp createddate;
	@Column( name = "createdby", nullable = false, updatable = false )
	private String createdby;
	public ProductCategory() {
		super();
		// TODO Auto-generated constructor stub
	}
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
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Set<ProductLineItem> getPliSet() {
		return pliSet;
	}
	public void setPliSet(Set<ProductLineItem> pliSet) {
		this.pliSet = pliSet;
	}
	public Timestamp getModifieddate() {
		return modifieddate;
	}
	public void setModifieddate(Timestamp modifieddate) {
		this.modifieddate = modifieddate;
	}
	public String getModifiedby() {
		return modifiedby;
	}
	public void setModifiedby(String modifiedby) {
		this.modifiedby = modifiedby;
	}
	public Timestamp getCreateddate() {
		return createddate;
	}
	public void setCreateddate(Timestamp createddate) {
		this.createddate = createddate;
	}
	public String getCreatedby() {
		return createdby;
	}
	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}
	
	

}