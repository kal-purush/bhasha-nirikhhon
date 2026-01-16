package com.javawebtutor;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Entity
public class Assignment implements Serializable {

	private User owner;
	private Integer id;
	private AssignmentExtension extension;

	public Assignment() {
	}

	public Assignment(User owner, Integer id) {
		this.owner = owner;
		this.id = id;
	}

	@Id
	@ManyToOne(fetch = FetchType.LAZY)
	public User getOwner() {
		return owner;
	}

	public void setOwner(User owner) {
		this.owner = owner;
	}

	@Id
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@OneToOne(mappedBy = "owner", cascade = CascadeType.ALL)
	private AssignmentExtension getExtension() {
		return extension;
	}

	public void setExtension(AssignmentExtension extension) {
		this.extension = extension;
	}
}