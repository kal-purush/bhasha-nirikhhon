/**
 *
 */
package com.boundlessgeo.ps.turbopsapi.model;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import lombok.Getter;
import lombok.Setter;

/**
 * A container for most miscellaneous stuff. Mostly focuses on id and audit
 * behaviors. If other behaviors need to be added to the child classes, then
 * consider creating interfaces for each behavior and mixing interfaces to
 * create new abstract objects that have the hybrid behaviors.
 *
 * @author ssengupta
 *
 */
@SuppressWarnings("serial")
@MappedSuperclass
public abstract class AuditableObject implements Serializable {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long id;

	@Getter
	@Setter
	@Temporal(TemporalType.TIMESTAMP)
	@Column(nullable = false)
	protected Date createdAt;

	@Getter
	@Setter
	@Temporal(TemporalType.TIMESTAMP)
	protected Date lastModifiedAt;

	@PrePersist
	protected void onCreate() {
		createdAt = Calendar.getInstance().getTime();
	}

	@PreUpdate
	protected void onUpdate() {
		lastModifiedAt = Calendar.getInstance().getTime();
	}
}