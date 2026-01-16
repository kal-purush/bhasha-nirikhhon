package com.oasis.poc1.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name ="Oasis_Poc1")
public class OasisPoc1 {
	
	@Id
	private Integer id;
	@Column(name ="label_name")
	private String labelName;
	@Column(name ="entity_name")
	private String entityName;
	private String value;
	@Column(name ="attribute_name")
	private String attribute;
	@Column(name ="saved_timestamp")
	private String timestamp;
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getLabelName() {
		return labelName;
	}
	public void setLabelName(String labelName) {
		this.labelName = labelName;
	}
	public String getEntityName() {
		return entityName;
	}
	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getAttribute() {
		return attribute;
	}
	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		return "OasisPoc1 [id=" + id + ", labelName=" + labelName + ", entityName=" + entityName + ", value=" + value
				+ ", attribute=" + attribute + ", timestamp=" + timestamp + "]";
	}
	
	
}