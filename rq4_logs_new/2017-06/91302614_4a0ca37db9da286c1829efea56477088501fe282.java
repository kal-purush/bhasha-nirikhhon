package com.quick.location.entity;

import java.io.Serializable;
import javax.persistence.*;

/**
 * The persistent class for the report database table.
 * 
 */
@Entity
@Table(name = "report")
@NamedQuery(name = "ReportEntity.findAll", query = "SELECT r FROM ReportEntity r")
public class ReportEntity implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "id_report")
	private String idReport;

	private String author;

	private String field;

	@Column(name = "field_human")
	private String fieldHuman;

	@Lob
	private String value;

	// bi-directional many-to-one association to PlaceEntity
	@ManyToOne
	@JoinColumn(name = "place_id")
	private PlaceEntity place;

	public void setIdReport(String idReport) {
		this.idReport = idReport;
	}

	public String getAuthor() {
		return this.author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getField() {
		return this.field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getFieldHuman() {
		return this.fieldHuman;
	}

	public void setFieldHuman(String fieldHuman) {
		this.fieldHuman = fieldHuman;
	}

	public String getValue() {
		return this.value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public PlaceEntity getPlace() {
		return this.place;
	}

	public void setPlace(PlaceEntity place) {
		this.place = place;
	}

}